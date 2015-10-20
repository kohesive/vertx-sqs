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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.properties.Delegates

class SqsSequentialQueueConsumerVerticle() : AbstractVerticle(), SqsVerticle {

    constructor(credentialsProvider: AWSCredentialsProvider) : this() {
        this.credentialsProvider = credentialsProvider
    }
    override var credentialsProvider: AWSCredentialsProvider? = null

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsSequentialQueueConsumerVerticle")

    private var pool : ExecutorService by Delegates.notNull()

    override fun start(startFuture: Future<Void>) {
        client = SqsClientImpl(vertx, config(), credentialsProvider)

        val queueUrl     = config().getString("queueUrl")
        val address      = config().getString("address")
        val workersCount = config().getInteger("workersCount")
        val timeout      = config().getLong("timeout") ?: SqsVerticle.DefaultTimeout

        pool = Executors.newFixedThreadPool(workersCount)

        client.start {
            if (it.succeeded()) {
                subscribe(queueUrl, address, workersCount, timeout)
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    private fun subscribe(queueUrl: String, address: String, workersCount: Int, timeout: Long) {
        val task = Runnable {
            while (true) {
                val latch = CountDownLatch(1)

                try {
                    client.receiveMessage(queueUrl) {
                        if (it.succeeded()) {
                            it.result().forEach { message ->
                                val reciept = message.getString("receiptHandle")

                                vertx.eventBus().send(address, message, DeliveryOptions().setSendTimeout(timeout), Handler { ar: AsyncResult<Message<Void>> ->
                                    if (ar.succeeded()) {
                                        // Had to code it like this, as otherwise I was getting 'bad enclosing class' from Java compiler
                                        deleteMessage(queueUrl, reciept)
                                    } else {
                                        log.warn("Message with receipt $reciept was failed to process by the consumer")
                                    }

                                    latch.countDown()
                                })
                            }
                        } else {
                            log.error("Unable to poll messages from $queueUrl", it.cause())
                            latch.countDown()
                        }
                    }
                } catch (t: Throwable) {
                    log.error("Error while polling messages from SQS queue")
                }

                latch.await(timeout + 100, TimeUnit.MILLISECONDS)
            }
        }


        (1..workersCount).forEach {
             // Can't inline here because of 'bad enclosing class' compiler error
            pool.execute(task)
        }
    }

    override fun stop(stopFuture: Future<Void>) {
        pool.shutdown()

        client.stop {
            if (it.succeeded()) {
                stopFuture.complete()
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }


}