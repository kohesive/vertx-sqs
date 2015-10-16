package org.collokia.vertx.sqs

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import kotlin.properties.Delegates

class SqsQueueProducerVerticle : AbstractVerticle(), SqsVerticle {

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsQueueProducerVerticle")

    override fun start(startFuture: Future<Void>) {
        client = SqsClient.create(vertx, config())

        val queueUrl = config().getString("queueUrl")
        val address  = config().getString("address")

        client.start {
            if (it.succeeded()) {
                // Start routing the messages
                val consumer = vertx.eventBus().consumer(address, Handler { message: Message<JsonObject> ->
                    client.sendMessage(queueUrl, address) {
                        if (it.succeeded()) {
                            message.reply(it.result())
                        } else {
                            message.fail(0, "Failed to submit SQS message: ${ it.cause()?.getMessage() }")
                        }
                    }
                })
                consumer.completionHandler {
                    if (it.succeeded()) {
                        startFuture.complete()
                    } else {
                        startFuture.fail(it.cause())
                    }
                }
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    override fun stop(stopFuture: Future<Void>) {
        client.stop {
            if (it.succeeded()) {
                stopFuture.complete()
            } else {
                stopFuture.fail(it.cause())
            }
        }
    }

}