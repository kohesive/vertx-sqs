package uy.kohesive.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import io.vertx.core.AbstractVerticle
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.logging.LoggerFactory
import uy.kohesive.vertx.sqs.impl.SqsClientImpl
import kotlin.properties.Delegates

class SqsQueueConsumerVerticle() : AbstractVerticle(), SqsVerticle {

    constructor(credentialsProvider: AWSCredentialsProvider) : this() {
        this.credentialsProvider = credentialsProvider
    }
    override var credentialsProvider: AWSCredentialsProvider? = null

    override var client: SqsClient by Delegates.notNull()
    override val log = LoggerFactory.getLogger("SqsQueueConsumerVerticle")

    private var timerId: Long = -1

    override fun start(startFuture: Promise<Void>) {
        client = SqsClientImpl(vertx, config(), credentialsProvider)

        val queueUrl    = config().getString("queueUrl")
        val address     = config().getString("address")
        val maxMessages = config().getInteger("messagesPerPoll") ?: 1
        val timeout     = config().getLong("timeout") ?: SqsVerticle.DefaultTimeout

        val pollingInterval = config().getLong("pollingInterval")

        client.start {
            if (it.succeeded()) {
                subscribe(pollingInterval, queueUrl, address, maxMessages, timeout)
                startFuture.complete()
            } else {
                startFuture.fail(it.cause())
            }
        }
    }

    private fun subscribe(pollingInterval: Long, queueUrl: String, address: String, maxMessages: Int, timeout: Long) {
        timerId = vertx.setPeriodic(pollingInterval) {
            client.receiveMessages(queueUrl, maxMessages) {
                if (it.succeeded()) {
                    log.debug("Polled ${it.result().size} messages")
                    it.result().forEach { message ->
                        val reciept = message.getString("receiptHandle")

                        vertx.eventBus().send(address, message, DeliveryOptions().setSendTimeout(timeout), Handler { ar: AsyncResult<Message<Void>> ->
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

    override fun stop(stopFuture: Promise<Void>) {
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