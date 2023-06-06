package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.asFlux
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * The role of the tool is to combine several identical calls into only one consumption
 * The tool builds a screener, and multiple processors
 * The screener assigns one task each unique input, and the processor triggers the job
 * and suspend the threads during the task.
 *
 * Each time the caller is invoked, the new input is compared with a currently executing task input.
 * If one of the currently executing inputs collides, then the matching task of the colliding input
 * is returned instead of a new task.
 * Then the caller waits for the task to complete, with either the result or the exception
 */
object PoolingClientStrategy {

    private val LOGGER = LoggerFactory.getLogger(PoolingClientStrategy::class.java)

    private val processorSupervisorJob = SupervisorJob()
    fun <Input, Output> CoroutineScope.processor(
        inputs: ReceiveChannel<Input>,
        processDone: SendChannel<Pair<Input, Output?>>,
        processFailed: SendChannel<Pair<Input, Throwable>>,
        counter: AtomicInteger,
        operation: suspend CoroutineScope.(Input) -> Output?
    ): Job {
        return launch {
            for (input in inputs) {
                launch(
                    CoroutineExceptionHandler { _, exception ->
                        launch {
                            processFailed.send(input to exception)
                        }
                    } + this.coroutineContext + processorSupervisorJob
                ) {
                    LOGGER.debug("call number ${counter.incrementAndGet()}")
                    val result = operation(input)
                    processDone.send(input to result)
                }
            }
        }
    }

    fun <Input, Output> CoroutineScope.screener(
        keyComputationFunction: (Input?) -> String?,
        receivedInputs: ReceiveChannel<Input>,
        taskIdNotifier: MutableSharedFlow<Pair<Input, UUID>>,
        processorSendChannel: SendChannel<Input>,
        processDoneChannel: ReceiveChannel<Pair<Input, Output?>>,
        processFailedChannel: ReceiveChannel<Pair<Input, Throwable>>,
        failureBroadcaster: MutableSharedFlow<Pair<UUID, Throwable>>,
        resultBroadcaster: MutableSharedFlow<Pair<UUID, Output?>>
    ) = launch {
        val requested = mutableMapOf<String, UUID>()
        while (true) {
            select<Unit> {
                receivedInputs.onReceive { input ->
                    val inputAsText = keyComputationFunction(input) ?: ""
                    if (requested[inputAsText] != null) {
                        taskIdNotifier.emit(input to requested[inputAsText]!!)
                    } else {
                        val newTaskId = UUID.randomUUID()
                        requested[inputAsText] = newTaskId
                        taskIdNotifier.emit(input to newTaskId)
                        processorSendChannel.send(input)
                    }
                }
                processFailedChannel.onReceive { (input, result) ->
                    failureBroadcaster.emit(requested[keyComputationFunction(input)]!! to result)
                    requested.remove(keyComputationFunction(input))
                }
                processDoneChannel.onReceive { (input, result) ->
                    resultBroadcaster.emit(requested[keyComputationFunction(input)]!! to result)
                    requested.remove(keyComputationFunction(input))
                }
            }
        }
    }

    inline fun <Input, Output> CoroutineScope.caller(
        howManyWorkers: Int = 10,
        noinline keyComputationFunction: (Input?) -> String? = { it?.toString() },
        crossinline operation: suspend CoroutineScope.(Input) -> Output? = { null }
    ): Caller<Input, Output> {
        val receivedInputs = Channel<Input>()
        val taskIdNotifier = MutableSharedFlow<Pair<Input, UUID>>(8192)
        val processorSendChannel = Channel<Input>()
        val processDoneChannel = Channel<Pair<Input, Output?>>()
        val processFailedChannel = Channel<Pair<Input, Throwable>>()
        val failureBroadcaster = MutableSharedFlow<Pair<UUID, Throwable>>(8192)
        val resultBroadcaster = MutableSharedFlow<Pair<UUID, Output?>>(8192)

        val counter = AtomicInteger(0)

        val jobs = (0 until howManyWorkers).map {
            processor(processorSendChannel, processDoneChannel, processFailedChannel, counter) {
                operation.invoke(this, it)
                run { operation.invoke(this, it) }
            }
        } + screener(
            keyComputationFunction,
            receivedInputs,
            taskIdNotifier,
            processorSendChannel,
            processDoneChannel,
            processFailedChannel,
            failureBroadcaster,
            resultBroadcaster
        )

        return Caller(
            this,
            receivedInputs,
            taskIdNotifier,
            resultBroadcaster,
            failureBroadcaster,
            keyComputationFunction,
            counter
        ) { jobs.map { it.cancel() } }
    }

    class PooledCallerStrategyException(val taskId: UUID, cause: Throwable) : Exception(cause)

    class Caller<Input, Output>(
        private val coroutineScope: CoroutineScope,
        private val sendInput: SendChannel<Input>,
        private val taskIdReader: MutableSharedFlow<Pair<Input, UUID>>,
        private val resultReader: MutableSharedFlow<Pair<UUID, Output?>>,
        private val failureReader: MutableSharedFlow<Pair<UUID, Throwable>>,
        private val keyComputationFunction: (Input?) -> String?,
        private val counter: AtomicInteger,
        private val cancel: suspend CoroutineScope.() -> Unit
    ) {

        fun callAndGroupBy(input: Input): Deferred<Output> = coroutineScope.async(SupervisorJob()) {
            var task: Pair<Input, UUID>?
            val taskIdSubscriber = taskIdReader
            val resultSubscription = resultReader
            val errorSubscription = failureReader

            flow<Result<Output>> {
                if (keyComputationFunction(input) == null) {
                    emit(Result.failure(IllegalArgumentException("could not group by $input")))
                    return@flow
                }

                sendInput.send(input)

                task =
                    taskIdSubscriber.asFlux(coroutineContext)
                        .filter { keyComputationFunction(input) == keyComputationFunction(it?.first) }
                        .awaitFirst()

                val taskId = task?.second
                if (taskId == null) {
                    emit(Result.failure(IllegalArgumentException("could not process the work with $input")))
                    return@flow
                }


                val returned = Flux.merge(
                    resultSubscription.asFlux(coroutineContext)
                        .map { Result.success(it) },
                    errorSubscription.asFlux(coroutineContext)
                        .map {
                            Result.failure<Pair<UUID, Output?>>(
                                PooledCallerStrategyException(
                                    it.first,
                                    it.second
                                )
                            )
                        }
                )
                    .filter {
                        if (it.isSuccess) return@filter it.getOrThrow().first == taskId
                        if (it.isFailure) return@filter (
                                it.exceptionOrNull()!! as PooledCallerStrategyException)
                            .taskId == taskId
                        return@filter false
                    }
                    .awaitFirst()

                emit(returned.map { it.second!! })
            }.take(1).first().getOrElse { throw it.cause!! }
        }

        fun invocationsCount() = counter.get()

        suspend fun stop() {
            cancel(coroutineScope)
        }
    }
}
