package vertx.io.sqs.impl

import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.CreateQueueRequest
import com.amazonaws.services.sqs.model.ListQueuesRequest
import com.amazonaws.services.sqs.model.SendMessageRequest
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import vertx.io.sqs.SqsClient

public class SqsClientImpl(val vertx: Vertx, val config: JsonObject) : SqsClient {

    companion object {
        private val log = LoggerFactory.getLogger(javaClass)
    }

    private var client: AmazonSQSAsyncClient? = null

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

    override fun start(resultHandler: Handler<AsyncResult<Void>>) {
        log.info("Starting SQS client");

        vertx.executeBlocking(Handler { future ->
            try {
                val credentials: AWSCredentials = try {
                    ProfileCredentialsProvider().getCredentials()
                } catch (t: Throwable) {
                    throw AmazonClientException(
                        "Cannot load the credentials from the credential profiles file. " +
                        "Please make sure that your credentials file is at the correct " +
                        "location (~/.aws/credentials), and is in valid format."
                    )
                }

                client = AmazonSQSAsyncClient(credentials)
                client?.setRegion(Region.getRegion(Regions.US_WEST_2)) // TODO: configure

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

    override fun stop(resultHandler: Handler<AsyncResult<Void>>?) {
        // nothing
    }

    fun <SqsRequest : AmazonWebServiceRequest, SqsResult, VertxResult>
            Handler<AsyncResult<VertxResult>>.withConverter(
            converter: (SqsResult) -> VertxResult
    ): SqsToVertxHandlerAdapter<SqsRequest, SqsResult, VertxResult> =
        SqsToVertxHandlerAdapter(
            vertxHandler            = this,
            sqsResultToVertxMapper  = converter
        )

    class SqsToVertxHandlerAdapter<SqsRequest : AmazonWebServiceRequest, SqsResult, VertxResult>(
        val vertxHandler: Handler<AsyncResult<VertxResult>>,
        val sqsResultToVertxMapper: (SqsResult) -> VertxResult
    ) : com.amazonaws.handlers.AsyncHandler<SqsRequest, SqsResult> {

        override fun onSuccess(request: SqsRequest, result: SqsResult) {
            vertxHandler.handle(Future.succeededFuture(sqsResultToVertxMapper(result)))
        }

        override fun onError(exception: Exception) {
            vertxHandler.handle(Future.failedFuture(exception))
        }

    }

}

