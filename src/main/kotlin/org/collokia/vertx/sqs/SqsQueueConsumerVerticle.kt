package org.collokia.vertx.sqs

import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.logging.LoggerFactory
import kotlin.properties.Delegates

class SqsQueueConsumerVerticle : AbstractVerticle(), SqsVerticle {

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsQueueConsumerVerticle")

    private var timerId: Long = -1

    override fun start(startFuture: Future<Void>) {
        client = SqsClient.create(vertx, config())

        val queueUrl    = config().getString("queueUrl")
        val address     = config().getString("address")
        val maxMessages = config().getInteger("messagesPerPoll") ?: 1

        val pollingInterval = config().getLong("pollingInterval")

        client.start {
            if (it.succeeded()) {
                subscribe(pollingInterval, queueUrl, address, maxMessages)
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    private fun subscribe(pollingInterval: Long, queueUrl: String, address: String, maxMessages: Int) {
        timerId = vertx.setPeriodic(pollingInterval) {
            client.receiveMessages(queueUrl, maxMessages) {
                if (it.succeeded()) {
                    log.debug("Polled ${it.result().size()} messages")
                    it.result().forEach { message ->
                        val reciept = message.getString("receiptHandle")

                        vertx.eventBus().send(address, message, Handler { ar: AsyncResult<Message<Void>> ->
                            if (ar.succeeded()) {
                                // Had to code it like this, as otherwise I was getting 'bad enclosing class' from Java compiler
                                deleteMessage(queueUrl, reciept)
                            } else {
                                log.warn("Message with receipt $reciept was failed to process by the consumer")
                            }
                        })
                    }
                } else {
                    log.error("Unable to poll messages from $queueUrl", it.cause())
                }
            }
        }
    }

    override fun stop(stopFuture: Future<Void>) {
        vertx.cancelTimer(timerId)
        client.stop {
            if (it.succeeded()) {
                stopFuture.complete()
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }
}