package org.toilelibre.libe.poolingclient

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

@UseExperimental(ExperimentalCoroutinesApi::class)
object PoolingClientStrategy {

    private val LOGGER = LoggerFactory.getLogger(PoolingClientStrategy::class.java)

    private val processorSupervisorJob = SupervisorJob()
    fun <Input, Output> CoroutineScope.processor(inputs: ReceiveChannel<Input>,
                                                 processDone: SendChannel<Pair<Input, Output?>>,
                                                 processFailed: SendChannel<Pair<Input, Throwable>>,
                                                 counter: AtomicInteger,
                                                 operation: suspend CoroutineScope.(Input) -> Output?) = launch {
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


    fun <Input, Output> CoroutineScope.screener(receivedInputs: ReceiveChannel<Input>,
                                                taskIdNotifier: SendChannel<Pair<Input, UUID>>,
                                                processorSendChannel: SendChannel<Input>,
                                                processDoneChannel: ReceiveChannel<Pair<Input, Output?>>,
                                                processFailedChannel: ReceiveChannel<Pair<Input, Throwable>>,
                                                failureBroadcaster: BroadcastChannel<Pair<UUID, Throwable>>,
                                                resultBroadcaster: BroadcastChannel<Pair<UUID, Output?>>) = launch {
        val requested = mutableMapOf<String, UUID>()
        while (true) {
            select<Unit> {
                receivedInputs.onReceive { input ->
                    val inputAsText = input.toString()
                    if (requested[inputAsText] != null) {
                        taskIdNotifier.send(input to requested[inputAsText]!!)
                    } else {
                        val newTaskId = UUID.randomUUID()
                        requested[inputAsText] = newTaskId
                        taskIdNotifier.send(input to newTaskId)
                        processorSendChannel.send(input)
                    }
                }
                processFailedChannel.onReceive { (input, result) ->
                    failureBroadcaster.send(requested[input.toString()]!! to result)
                    requested.remove(input.toString())
                }
                processDoneChannel.onReceive { (input, result) ->
                    resultBroadcaster.send(requested[input.toString()]!! to result)
                    requested.remove(input.toString())
                }
            }
        }
    }

    inline fun <Input, Output> CoroutineScope.caller(howManyWorkers: Int = 3, crossinline operation: suspend CoroutineScope.(Input) -> Output? = { null }): Caller<Input, Output> {
        val receivedInputs = Channel<Input>()
        val taskIdNotifier = Channel<Pair<Input, UUID>>()
        val processorSendChannel = Channel<Input>()
        val processDoneChannel = Channel<Pair<Input, Output?>>()
        val processFailedChannel = Channel<Pair<Input, Throwable>>()
        val failureBroadcaster = BroadcastChannel<Pair<UUID, Throwable>>(512)
        val resultBroadcaster = BroadcastChannel<Pair<UUID, Output?>>(512)

        val counter = AtomicInteger(0)

        val jobs =
              listOf(screener(receivedInputs, taskIdNotifier, processorSendChannel, processDoneChannel,
                        processFailedChannel, failureBroadcaster, resultBroadcaster)) +
        (0 until howManyWorkers).map {
            processor(processorSendChannel, processDoneChannel, processFailedChannel, counter) {
                 operation.invoke(this, it)
            }
        }

        return Caller(receivedInputs, taskIdNotifier, resultBroadcaster, failureBroadcaster, counter, jobs)
    }

    class Caller<Input, Output>(private val sendInput: Channel<Input>,
                                private val taskIdReader: Channel<Pair<Input, UUID>>,
                                private val resultReader: BroadcastChannel<Pair<UUID, Output?>>,
                                private val failureReader: BroadcastChannel<Pair<UUID, Throwable>>,
                                private val counter: AtomicInteger,
                                private val jobs: List<Job>) : CoroutineScope {

        private val job = SupervisorJob()
        override val coroutineContext get() = job + Dispatchers.IO

        fun callAndGroupBy(input: Input): Deferred<Output?> = async {
            sendInput.send(input)

            var task: Pair<Input, UUID>? = null

            while (task?.first != input) {
                task = taskIdReader.receive()
            }

            val taskId = task!!.second

            val resultSubscription = resultReader.openSubscription()
            val errorSubscription = failureReader.openSubscription()
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
            if (error?.first == taskId){
                throw error!!.second
            }
            received!!.second
        }

        fun invocationsCount () = counter.get()

        fun stop() {
            jobs.map { it.cancel() }
        }
    }
}