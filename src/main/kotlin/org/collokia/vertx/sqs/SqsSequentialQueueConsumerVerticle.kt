package org.collokia.vertx.sqs

import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.logging.LoggerFactory
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.properties.Delegates

class SqsSequentialQueueConsumerVerticle : AbstractVerticle(), SqsVerticle {

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsSequentialQueueConsumerVerticle")

    private var pool : ExecutorService by Delegates.notNull()

    override fun start(startFuture: Future<Void>) {
        client = SqsClient.create(vertx, config())

        val queueUrl     = config().getString("queueUrl")
        val address      = config().getString("address")
        val workersCount = config().getInteger("workersCount")

        pool = Executors.newFixedThreadPool(workersCount)

        client.start {
            if (it.succeeded()) {

                pool.invokeAll((1..workersCount).map {
                    Callable {
                        while (true) {
                            val latch = CountDownLatch(1)

                            client.receiveMessage(queueUrl) {
                                if (it.succeeded()) {
                                    it.result().forEach { message ->
                                        val reciept = message.getString("receiptHandle")

                                        vertx.eventBus().send(address, message, Handler { ar: AsyncResult<Message<Void>> ->
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

                            latch.await()
                        }
                    }
                })

                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
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