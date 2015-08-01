package io.vertx.sqs

import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory

class SqsQueueConsumerVerticle : AbstractVerticle() {

    companion object {
        private val log = LoggerFactory.getLogger("SqsQueueConsumerVerticle")
    }

    private var client: SqsClient? = null
    private var timerId: Long = -1

    override fun start(startFuture: Future<Void>) {
        client = SqsClient.create(vertx, config())

        val queueUrl = config().getString("queueUrl")
        val address  = config().getString("address")

        val pollingInterval = config().getLong("pollingInterval")

        client?.start {
            if (it.succeeded()) {
                subscribe(pollingInterval, queueUrl, address)
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    private fun subscribe(pollingInterval: Long, queueUrl: String, address: String) {
        timerId = vertx.setPeriodic(pollingInterval) {
            client?.receiveMessage(queueUrl, Handler {
                if (it.succeeded()) {
                    log.debug("Polled ${it.result().size()} messages")
                    it.result().forEach { message ->
                        val reciept = message.getString("receiptHandle")

                        vertx.eventBus().send(address, message, Handler { ar: AsyncResult<Message<Void>> ->
                            if (ar.succeeded()) {
                                client?.deleteMessage(queueUrl, reciept) {
                                    if (it.failed()) {
                                        log.warn("Unable to acknowledge message deletion with receipt = $reciept")
                                    }
                                }
                            } else {
                                log.warn("Message with receipt $reciept was failed to process by the consumer")
                            }
                        })
                    }
                } else {
                    log.error("Unable to poll messages from $queueUrl", it.cause())
                }
            })
        }
    }

    override fun stop(stopFuture: Future<Void>) {
        vertx.cancelTimer(timerId)
        client?.stop {
            if (it.succeeded()) {
                stopFuture.complete()
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }
}