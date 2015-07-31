package io.vertx.sqs.impl

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.*
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.sqs.SqsClient

public class SqsClientImpl(val vertx: Vertx, val config: JsonObject) : SqsClient {

    companion object {
        private val log = LoggerFactory.getLogger(javaClass)
    }

    private var client: AmazonSQSAsyncClient? = null

    override fun sendMessage(queueUrl: String, messageBody: String, resultHandler: Handler<AsyncResult<String>>) {
        sendMessage(queueUrl, messageBody, null, resultHandler)
    }

    override fun sendMessage(queueUrl: String, messageBody: String, delaySeconds: Int?, resultHandler: Handler<AsyncResult<String>>) {
        withClient { client ->
            val request = SendMessageRequest(queueUrl, messageBody).withDelaySeconds(delaySeconds)
            client.sendMessageAsync(request, resultHandler.withConverter { sqsResult ->
                sqsResult.getMessageId()
            })
        }
    }

    override fun createQueue(name: String, attributes: MutableMap<String, String>, resultHandler: Handler<AsyncResult<String>>) {
        withClient { client ->
            client.createQueueAsync(CreateQueueRequest(name).withAttributes(attributes), resultHandler.withConverter { sqsResult ->
                sqsResult.getQueueUrl()
            })
        }
    }

    override fun listQueues(namePrefix: String?, resultHandler: Handler<AsyncResult<List<String>>>) {
        withClient { client ->
            client.listQueuesAsync(ListQueuesRequest(namePrefix), resultHandler.withConverter { sqsResult ->
                sqsResult.getQueueUrls()
            })
        }
    }

    override fun receiveMessage(queueUrl: String, resultHandler: Handler<AsyncResult<List<JsonObject>>>) {
        withClient { client ->
            client.receiveMessageAsync(ReceiveMessageRequest(queueUrl), resultHandler.withConverter { sqsResult ->
                sqsResult.getMessages().map {
                    it.toJsonObject()
                }
            })
        }
    }

    override fun deleteMessage(queueUrl: String, receiptHandle: String, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client ->
            client.deleteMessageAsync(DeleteMessageRequest(queueUrl, receiptHandle), resultHandler.toSqsHandler())
        }
    }

    // TODO: attributes
    private fun Message.toJsonObject(): JsonObject = JsonObject()
        .put("id", this.getMessageId())
        .put("body", this.getBody())
        .put("bodyMd5", this.getMD5OfBody())
        .put("receiptHandle", this.getReceiptHandle())

    override fun start(resultHandler: Handler<AsyncResult<Void>>) {
        log.info("Starting SQS client");

        vertx.executeBlocking(Handler { future ->
            try {
                val credentials: AWSCredentials = if (config.getString("accessKey") != null) {
                    BasicAWSCredentials(config.getString("accessKey"), config.getString("secretKey"))
                } else {
                    try {
                        ProfileCredentialsProvider().getCredentials()
                    } catch (t: Throwable) {
                        throw AmazonClientException(
                            "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct "  +
                            "location (~/.aws/credentials), and is in valid format."
                        )
                    }
                }

                client = AmazonSQSAsyncClient(credentials)

                client?.setRegion(Region.getRegion(Regions.fromName(config.getString("region"))))
                if (config.getString("host") != null && config.getInteger("port") != null) {
                    client?.setEndpoint("http://${ config.getString("host") }:${ config.getInteger("port") }")
                }

                future.complete()
            } catch (t: Throwable) {
                future.fail(t)
            }
        }, true, resultHandler)
    }

    private fun withClient(handler: (AmazonSQSAsyncClient) -> Unit) {
        val theClient = client
        if (theClient != null) {
            handler(theClient)
        } else {
            throw IllegalStateException("SQS client wasn't initialized")
        }
    }

    override fun stop(resultHandler: Handler<AsyncResult<Void>>) {
        resultHandler.handle(Future.succeededFuture()) // nothing
    }

    fun <SqsRequest : AmazonWebServiceRequest> Handler<AsyncResult<Void?>>.toSqsHandler(): AsyncHandler<SqsRequest, Void?> = withConverter { it }

    fun <SqsRequest : AmazonWebServiceRequest, SqsResult, VertxResult> Handler<AsyncResult<VertxResult>>.withConverter(
            converter: (SqsResult) -> VertxResult
    ): SqsToVertxHandlerAdapter<SqsRequest, SqsResult, VertxResult> =
        SqsToVertxHandlerAdapter(
            vertxHandler            = this,
            sqsResultToVertxMapper  = converter
        )

    class SqsToVertxHandlerAdapter<SqsRequest : AmazonWebServiceRequest, SqsResult, VertxResult>(
        val vertxHandler: Handler<AsyncResult<VertxResult>>,
        val sqsResultToVertxMapper: (SqsResult) -> VertxResult
    ) : AsyncHandler<SqsRequest, SqsResult> {

        override fun onSuccess(request: SqsRequest, result: SqsResult) {
            vertxHandler.handle(Future.succeededFuture(sqsResultToVertxMapper(result)))
        }

        override fun onError(exception: Exception) {
            vertxHandler.handle(Future.failedFuture(exception))
        }
    }

}

