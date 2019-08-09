package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@ExperimentalCoroutinesApi
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
                launch(CoroutineExceptionHandler { _, exception ->
                    launch {
                        processFailed.send(input to exception)
                    }
                } + this.coroutineContext + processorSupervisorJob) {
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
            taskIdNotifier: BroadcastChannel<Pair<Input, UUID>>,
            processorSendChannel: SendChannel<Input>,
            processDoneChannel: ReceiveChannel<Pair<Input, Output?>>,
            processFailedChannel: ReceiveChannel<Pair<Input, Throwable>>,
            failureBroadcaster: BroadcastChannel<Pair<UUID, Throwable>>,
            resultBroadcaster: BroadcastChannel<Pair<UUID, Output?>>
    ) = launch {
        val requested = mutableMapOf<String, UUID>()
        while (true) {
            select<Unit> {
                receivedInputs.onReceive { input ->
                    val inputAsText = keyComputationFunction(input)
                    if (requested[inputAsText] != null) {
                        taskIdNotifier.send(input to requested[inputAsText]!!)
                    } else {
                        val newTaskId = UUID.randomUUID()
                        requested[inputAsText ?: ""] = newTaskId
                        taskIdNotifier.send(input to newTaskId)
                        processorSendChannel.send(input)
                    }
                }
                processFailedChannel.onReceive { (input, result) ->
                    failureBroadcaster.send(requested[keyComputationFunction(input)]!! to result)
                    requested.remove(input.toString())
                }
                processDoneChannel.onReceive { (input, result) ->
                    resultBroadcaster.send(requested[keyComputationFunction(input)]!! to result)
                    requested.remove(input.toString())
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
        val taskIdNotifier = BroadcastChannel<Pair<Input, UUID>>(8192)
        val processorSendChannel = Channel<Input>()
        val processDoneChannel = Channel<Pair<Input, Output?>>()
        val processFailedChannel = Channel<Pair<Input, Throwable>>()
        val failureBroadcaster = BroadcastChannel<Pair<UUID, Throwable>>(8192)
        val resultBroadcaster = BroadcastChannel<Pair<UUID, Output?>>(8192)

        val counter = AtomicInteger(0)

        val jobs = (0 until howManyWorkers).map {
            processor(processorSendChannel, processDoneChannel, processFailedChannel, counter) {
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

        return Caller(this, receivedInputs, taskIdNotifier, resultBroadcaster, failureBroadcaster, keyComputationFunction,
                      counter, { jobs.map { it.cancel() } })
    }

    class Caller<Input, Output>(
            private val coroutineScope: CoroutineScope,
            private val sendInput: SendChannel<Input>,
            private val taskIdReader: BroadcastChannel<Pair<Input, UUID>>,
            private val resultReader: BroadcastChannel<Pair<UUID, Output?>>,
            private val failureReader: BroadcastChannel<Pair<UUID, Throwable>>,
            private val keyComputationFunction: (Input?) -> String?,
            private val counter: AtomicInteger,
            private val cancel: suspend CoroutineScope.() -> Unit
    ) {

        fun callAndGroupBy(input: Input): Deferred<Output?> = coroutineScope.async {
            var task: Pair<Input, UUID>? = null
            val taskIdSubscriber = taskIdReader.openSubscription()
            val resultSubscription = resultReader.openSubscription()
            val errorSubscription = failureReader.openSubscription()
            sendInput.send(input)

            while (keyComputationFunction(input) != keyComputationFunction(task?.first)) {
                task = taskIdSubscriber.receive()
            }
            taskIdSubscriber.cancel()

            val taskId = task!!.second

            var received: Pair<UUID, Output?>? = null
            var error: Pair<UUID, Throwable>? = null

            while (received?.first != taskId && error?.first != taskId) {
                select<Unit> {
                    resultSubscription.onReceive { result ->
                        received = result
                    }
                    errorSubscription.onReceive { throwable ->
                        error = throwable
                    }
                }
            }

            resultSubscription.cancel()
            errorSubscription.cancel()
            if (error?.first == taskId) {
                throw error!!.second
            }
            received!!.second
        }

        fun invocationsCount () = counter.get()

        suspend fun stop() {
            cancel(coroutineScope)
        }
    }
}