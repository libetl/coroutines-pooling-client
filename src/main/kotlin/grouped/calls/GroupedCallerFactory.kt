package grouped.calls

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
object GroupedCallerFactory {

    private val LOGGER = LoggerFactory.getLogger(GroupedCallerFactory::class.java)

    fun <Input, Output> CoroutineScope.processor(inputs: ReceiveChannel<Input>,
                                                 processDone: SendChannel<Pair<Input, Output?>>,
                                                 counter: AtomicInteger,
                                                 operation: suspend CoroutineScope.(Input) -> Output?) {
        launch {
            for (input in inputs) {
                launch {
                    LOGGER.debug("call number ${counter.incrementAndGet()}")
                    val result = operation(input)
                    processDone.send(input to result)
                }
            }
        }
    }


    fun <Input, Output> CoroutineScope.screener(receivedInputs: ReceiveChannel<Input>,
                                                taskIdNotifier: SendChannel<Pair<Input, UUID>>,
                                                processorSendChannel: SendChannel<Input>,
                                                processDoneChannel: ReceiveChannel<Pair<Input, Output?>>,
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
        val resultBroadcaster = BroadcastChannel<Pair<UUID, Output?>>(64)

        val counter = AtomicInteger(0)

        screener(receivedInputs, taskIdNotifier, processorSendChannel, processDoneChannel, resultBroadcaster)
        repeat(howManyWorkers) {
            processor(processorSendChannel, processDoneChannel, counter) {
                run { operation.invoke(this, it) }
            }
        }

        return Caller(receivedInputs, taskIdNotifier, resultBroadcaster)
    }

    class Caller<Input, Output>(private val sendInput: SendChannel<Input>, private val taskIdReader: ReceiveChannel<Pair<Input, UUID>>,
                                private val resultReader: BroadcastChannel<Pair<UUID, Output?>>) : CoroutineScope {

        private val job = Job()
        override val coroutineContext get() = job + Dispatchers.IO

        fun callAndGroupBy(input: Input): Deferred<Output?> = async {
            sendInput.send(input)

            var task: Pair<Input, UUID>? = null

            while (task?.first != input) {
                task = taskIdReader.receive()
            }

            val taskId = task!!.second

            val subscription = resultReader.openSubscription()
            var received: Pair<UUID, Output?>? = null

            while (received?.first != taskId) {
                received = subscription.receive()
            }

            subscription.cancel()
            received.second
        }
    }
}