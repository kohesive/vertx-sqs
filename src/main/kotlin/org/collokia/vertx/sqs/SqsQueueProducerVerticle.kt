package org.collokia.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import org.collokia.vertx.sqs.impl.SqsClientImpl
import kotlin.properties.Delegates

class SqsQueueProducerVerticle() : AbstractVerticle(), SqsVerticle {

    constructor(credentialsProvider: AWSCredentialsProvider) : this() {
        this.credentialsProvider = credentialsProvider
    }
    override var credentialsProvider: AWSCredentialsProvider? = null

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsQueueProducerVerticle")

    override fun start(startFuture: Future<Void>) {
        client = SqsClientImpl(vertx, config(), credentialsProvider)

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