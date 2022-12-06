package uy.kohesive.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import io.vertx.core.AbstractVerticle
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import mu.KotlinLogging
import uy.kohesive.vertx.sqs.impl.SqsClientImpl
import kotlin.properties.Delegates

class SqsQueueProducerVerticle() : AbstractVerticle(), SqsVerticle {

    constructor(credentialsProvider: AWSCredentialsProvider) : this() {
        this.credentialsProvider = credentialsProvider
    }
    override var credentialsProvider: AWSCredentialsProvider? = null

    override var client: SqsClient by Delegates.notNull()
    override val log = KotlinLogging.logger {}

    override fun start(startPromise: Promise<Void>) {
        client = SqsClientImpl(vertx, config(), credentialsProvider)

        val queueUrl = config().getString("queueUrl")
        val address  = config().getString("address")
        val local    = config().getBoolean("local") ?: false

        client.start {
            if (it.succeeded()) {
                // Start routing the messages
                val replyHandler = Handler { message: Message<String> ->
                    client.sendMessage(queueUrl, message.body()) {
                        if (it.succeeded()) {
                            message.reply(it.result())
                        } else {
                            message.fail(0, "Failed to submit SQS message: ${ it.cause()?.message }")
                        }
                    }
                }

                val consumer = if (local) {
                    vertx.eventBus().localConsumer(address, replyHandler)
                } else {
                    vertx.eventBus().consumer(address, replyHandler)
                }
                consumer.completionHandler {
                    if (it.succeeded()) {
                        startPromise.complete()
                    } else {
                        startPromise.fail(it.cause())
                    }
                }
            } else {
                startPromise.fail(it.cause())
            }
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        client.stop {
            if (it.succeeded()) {
                stopPromise.complete()
            } else {
                stopPromise.fail(it.cause())
            }
        }
    }

}