package org.collokia.vertx.sqs.impl

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.auth.AWSCredentialsProvider
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
import org.collokia.vertx.sqs.SqsClient
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.properties.Delegates

public class SqsClientImpl(val vertx: Vertx, val config: JsonObject, val credentialProvider: AWSCredentialsProvider? = null) : SqsClient {

    companion object {
        private val log = LoggerFactory.getLogger(SqsClientImpl::class.java)
    }

    private var client: AmazonSQSAsyncClient by Delegates.notNull()

    private var initialized = AtomicBoolean(false)

    private fun getCredentialsProvider(): AWSCredentialsProvider = credentialProvider ?: if (config.getString("accessKey") != null) {
        object : AWSCredentialsProvider {
            override fun getCredentials() = BasicAWSCredentials(config.getString("accessKey"), config.getString("secretKey"))
            override fun refresh() {}
        }
    } else {
        try {
            ProfileCredentialsProvider()
        } catch (t: Throwable) {
            throw AmazonClientException(
                "Cannot load the credentials from the credential profiles file. " +
                "Please make sure that your credentials file is at the correct " +
                "location (~/.aws/credentials), and is in valid format."
            )
        }
    }

    override fun sendMessage(queueUrl: String, messageBody: String, resultHandler: Handler<AsyncResult<String>>) {
        sendMessage(queueUrl, messageBody, null, null, resultHandler)
    }

    override fun sendMessage(queueUrl: String, messageBody: String, attributes: JsonObject?, resultHandler: Handler<AsyncResult<String>>) {
        sendMessage(queueUrl, messageBody, attributes, null, resultHandler)
    }

    override fun sendMessage(queueUrl: String, messageBody: String, attributes: JsonObject?, delaySeconds: Int?, resultHandler: Handler<AsyncResult<String>>) {
        withClient { client ->
            val request = SendMessageRequest(queueUrl, messageBody).withDelaySeconds(delaySeconds)

            request.messageAttributes = attributes?.map?.mapValues {
                (it.value as? JsonObject)?.let {
                    val type       = it.getString("dataType")
                    val stringData = it.getString("stringData")
                    val binaryData = it.getBinary("binaryData")

                    MessageAttributeValue().apply {
                        if (binaryData != null) {
                            binaryValue = ByteBuffer.wrap(binaryData)
                        }
                        if (stringData != null) {
                            stringValue = stringData
                        }
                        dataType = type
                    }
                }
            }

            client.sendMessageAsync(request, resultHandler.withConverter { sqsResult ->
                sqsResult.messageId
            })
        }
    }

    override fun createQueue(name: String, attributes: MutableMap<String, String>, resultHandler: Handler<AsyncResult<String>>) {
        withClient { client ->
            client.createQueueAsync(CreateQueueRequest(name).withAttributes(attributes), resultHandler.withConverter { sqsResult ->
                sqsResult.queueUrl
            })
        }
    }

    override fun listQueues(namePrefix: String?, resultHandler: Handler<AsyncResult<List<String>>>) {
        withClient { client ->
            client.listQueuesAsync(ListQueuesRequest(namePrefix), resultHandler.withConverter { sqsResult ->
                sqsResult.queueUrls
            })
        }
    }

    override fun receiveMessage(queueUrl: String, resultHandler: Handler<AsyncResult<List<JsonObject>>>) {
        receiveMessages(queueUrl, 1, resultHandler)
    }

    override fun receiveMessages(queueUrl: String, maxMessages: Int, resultHandler: Handler<AsyncResult<List<JsonObject>>>) {
        withClient { client ->
            client.receiveMessageAsync(ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(maxMessages), resultHandler.withConverter { sqsResult ->
                sqsResult.messages.map {
                    it.toJsonObject()
                }
            })
        }
    }

    override fun deleteQueue(queueUrl: String, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client ->
            client.deleteQueueAsync(DeleteQueueRequest(queueUrl), resultHandler.toSqsHandler())
        }
    }

//    override fun purgeQueue(queueUrl: String, resultHandler: Handler<AsyncResult<Void?>>) {
//        withClient { client ->
//            client.purgeQueueAsync(PurgeQueueRequest(queueUrl), resultHandler.toSqsHandler())
//        }
//    }

    override fun deleteMessage(queueUrl: String, receiptHandle: String, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client ->
            client.deleteMessageAsync(DeleteMessageRequest(queueUrl, receiptHandle), resultHandler.toSqsHandler())
        }
    }

    override fun setQueueAttributes(queueUrl: String, attributes: MutableMap<String, String>, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client ->
            client.setQueueAttributesAsync(SetQueueAttributesRequest(queueUrl, attributes), resultHandler.toSqsHandler())
        }
    }

    override fun changeMessageVisibility(queueUrl: String, receiptHandle: String, visibilityTimeout: Int, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client ->
            client.changeMessageVisibilityAsync(ChangeMessageVisibilityRequest(queueUrl, receiptHandle, visibilityTimeout), resultHandler.toSqsHandler())
        }
    }

    override fun getQueueUrl(queueName: String, queueOwnerAWSAccountId: String?, resultHandler: Handler<AsyncResult<String>>) {
        withClient { client ->
            client.getQueueUrlAsync(GetQueueUrlRequest(queueName).withQueueOwnerAWSAccountId(queueOwnerAWSAccountId), resultHandler.withConverter {
                it.queueUrl
            })
        }
    }

    override fun addPermissionAsync(queueUrl: String, label: String, aWSAccountIds: List<String>?, actions: List<String>?, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client ->
            client.addPermissionAsync(AddPermissionRequest(queueUrl, label, aWSAccountIds, actions), resultHandler.toSqsHandler())
        }
    }

    override fun removePermission(queueUrl: String, label: String, resultHandler: Handler<AsyncResult<Void?>>) {
        withClient { client -> 
            client.removePermissionAsync(RemovePermissionRequest(queueUrl, label), resultHandler.toSqsHandler())
        }
    }

    override fun getQueueAttributes(queueUrl: String, attributeNames: List<String>?, resultHandler: Handler<AsyncResult<JsonObject>>) {
        withClient { client ->
            client.getQueueAttributesAsync(GetQueueAttributesRequest(queueUrl, attributeNames), resultHandler.withConverter {
                JsonObject(it.attributes as Map<String, Any>?)
            })
        }
    }

    override fun listDeadLetterSourceQueues(queueUrl: String, resultHandler: Handler<AsyncResult<List<String>>>) {
        withClient { client ->
            client.listDeadLetterSourceQueuesAsync(ListDeadLetterSourceQueuesRequest().apply { setQueueUrl(queueUrl) }, resultHandler.withConverter {
                it.queueUrls
            })
        }
    }

    private fun Message.toJsonObject(): JsonObject = JsonObject()
        .put("id", this.messageId)
        .put("body", this.body)
        .put("bodyMd5", this.mD5OfBody)
        .put("receiptHandle", this.receiptHandle)
        .put("attributes", JsonObject(this.attributes as Map<String, Any>?))
        .put("messageAttributes", JsonObject(
            this.messageAttributes.mapValues { messageAttribute -> JsonObject()
                .put("dataType", messageAttribute.value.dataType)
                .apply {
                    if (messageAttribute.value.binaryValue != null) {
                        this.put("binaryData", messageAttribute.value.binaryValue.let {
                            it.clear()
                            val byteArray = ByteArray(it.capacity())
                            it.get(byteArray)
                            byteArray
                        })
                    } else {
                        this.put("stringData", messageAttribute.value.stringValue)
                    }
                }
            }
        ))


    override fun start(resultHandler: Handler<AsyncResult<Void>>) {
        log.info("Starting SQS client");

        vertx.executeBlocking(Handler { future ->
            try {
                client = AmazonSQSAsyncClient(getCredentialsProvider())

                val region = config.getString("region")
                client.setRegion(Region.getRegion(Regions.fromName(region)))
                if (config.getString("host") != null && config.getInteger("port") != null) {
                    client.setEndpoint("http://${ config.getString("host") }:${ config.getInteger("port") }")
                }

                initialized.set(true)

                future.complete()
            } catch (t: Throwable) {
                future.fail(t)
            }
        }, true, resultHandler)
    }

    private fun withClient(handler: (AmazonSQSAsyncClient) -> Unit) {
        if (initialized.get()) {
            handler(client)
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

