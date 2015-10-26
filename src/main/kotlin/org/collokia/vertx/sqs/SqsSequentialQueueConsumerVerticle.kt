package org.collokia.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.logging.LoggerFactory
import org.collokia.vertx.sqs.impl.SqsClientImpl
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.properties.Delegates

class SqsSequentialQueueConsumerVerticle() : AbstractVerticle(), SqsVerticle {

    constructor(credentialsProvider: AWSCredentialsProvider) : this() {
        this.credentialsProvider = credentialsProvider
    }
    override var credentialsProvider: AWSCredentialsProvider? = null

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsSequentialQueueConsumerVerticle")

    private var pollingPool: ExecutorService by Delegates.notNull()
    private var routingPool: ExecutorService by Delegates.notNull()

    override fun start(startFuture: Future<Void>) {
        client = SqsClientImpl(vertx, config(), credentialsProvider)

        val queueUrl        = config().getString("queueUrl")
        val address         = config().getString("address")
        val workersCount    = config().getInteger("workersCount")
        val timeout         = config().getLong("timeout") ?: SqsVerticle.DefaultTimeout
        val bufferSize      = config().getInteger("bufferSize") ?: (workersCount * 10)
        val pollingInterval = config().getLong("pollingInterval") ?: 1000

        routingPool = Executors.newFixedThreadPool(workersCount)
        pollingPool = Executors.newSingleThreadExecutor()

        client.start {
            if (it.succeeded()) {
                subscribe(queueUrl, address, workersCount, timeout, bufferSize, pollingInterval)
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    private fun subscribe(queueUrl: String, address: String, workersCount: Int, timeout: Long, bufferSize: Int, pollingInterval: Long) {
        val buffer = LinkedBlockingQueue<SqsMessage>()

        pollingPool.execute {
            while (true) {
                val latch      = CountDownLatch(1)
                val emptyQueue = AtomicBoolean(false)

                client.receiveMessages(queueUrl, bufferSize) {
                    try {
                        if (it.succeeded()) {
                            val messages = it.result()
                            if (messages.isEmpty()) {
                                emptyQueue.set(true)
                            } else {
                                it.result().map { jsonMessage ->
                                    SqsMessage(
                                        receipt = jsonMessage.getString("receiptHandle"),
                                        message = jsonMessage
                                    )
                                }.forEach {
                                    buffer.offer(it)
                                }
                            }
                        } else {
                            log.error("Can't poll messages from $queueUrl", it.cause())
                        }
                    } finally {
                        latch.countDown()
                    }
                }

                latch.await()

                if (emptyQueue.get()) {
                    Thread.sleep(5000)
                } else {
                    Thread.sleep(pollingInterval)
                }
            }
        }

        // Can't inline here because of 'bad enclosing class' compiler error
        val routingTask = {
            while (true) {
                val sqsMessage = buffer.take()
                val latch = CountDownLatch(1)

                vertx.eventBus().send(address, sqsMessage.message, DeliveryOptions().setSendTimeout(timeout), Handler { ar: AsyncResult<Message<Void?>> ->
                    if (ar.succeeded()) {
                        // Had to code it like this, as otherwise I was getting 'bad enclosing class' from Java compiler
                        deleteMessage(queueUrl, sqsMessage.receipt)
                    } else {
                        log.warn("Message with receipt ${ sqsMessage.receipt } was failed to process by the consumer")
                    }

                    latch.countDown()
                })

                latch.await(100 + timeout, TimeUnit.MILLISECONDS)
            }
        }
        (1..workersCount).forEach {
            routingPool.execute(routingTask)
        }
    }

    override fun stop(stopFuture: Future<Void>) {
        routingPool.shutdown()
        pollingPool.shutdown()

        client.stop {
            if (it.succeeded()) {
                stopFuture.complete()
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }


}