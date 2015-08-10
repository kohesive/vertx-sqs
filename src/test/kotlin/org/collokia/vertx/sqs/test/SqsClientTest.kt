package org.collokia.vertx.sqs.test

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.collokia.vertx.sqs.SqsClient
import org.elasticmq.rest.sqs.SQSRestServer
import org.elasticmq.rest.sqs.SQSRestServerBuilder
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import java.util.Arrays
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.platform.platformStatic
import kotlin.properties.Delegates

@RunWith(VertxUnitRunner::class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class SqsClientTest {

    companion object {
        val vertx: Vertx = Vertx.vertx()

        val ElasticMqPort = 9324
        val ElasticMqHost = "localhost"

        fun getQueueUrl(queueName: String) = "http://$ElasticMqHost:$ElasticMqPort/queue/$queueName"

        private var client: SqsClient by Delegates.notNull()
        private var sqsServer: SQSRestServer by Delegates.notNull()

        @BeforeClass
        @platformStatic
        fun before(context: TestContext) {
            sqsServer = SQSRestServerBuilder.withPort(ElasticMqPort).start()

            client = SqsClient.create(vertx, JsonObject(mapOf(
                "host"      to ElasticMqHost,
                "port"      to ElasticMqPort,
                "accessKey" to "someAccessKey",
                "secretKey" to "someSecretKey",
                "region"    to "us-west-2"
            )))

            val latch = CountDownLatch(1)
            client.start(context.asyncAssertSuccess { latch.countDown() })
            latch.await(10, TimeUnit.SECONDS)
        }

        @AfterClass
        @platformStatic
        fun after(context: TestContext) {
            client.stop(context.asyncAssertSuccess())
            vertx.close(context.asyncAssertSuccess())

            sqsServer.stopAndWait()
        }
    }

    @Test
    fun testCreateAndListQueue(context: TestContext) {
        context.withClient { client ->
            client.createQueue("testQueue", mapOf(), context.asyncAssertSuccess {
                client.listQueues(null, context.asyncAssertSuccess { queues ->
                    println(queues)
                    context.assertTrue(queues.firstOrNull { it == getQueueUrl("testQueue") } != null)
                })
            })
        }
    }

    @Test
    fun testSendReceiveAndDelete(context: TestContext) {
        val queueName   = getQueueUrl("testQueue")
        val messageBody = "Test message"

        context.withClient { client ->
            // Send

            val stringAttribute = "someString"
            val binaryAttribute = stringAttribute.toByteArray("UTF-8")

            val attributes = JsonObject()
                .put("stringAttribute", JsonObject().put("dataType", "String").put("stringData", stringAttribute))
                // TODO: uncomment when ElasticMQ supports binary attributes https://github.com/adamw/elasticmq/pull/54
//                .put("binaryAttribute", JsonObject().put("dataType", "Binary").put("binaryData", binaryAttribute))

            client.sendMessage(queueName, messageBody, attributes, context.asyncAssertSuccess {
                // Receive
                client.receiveMessage(queueName, context.asyncAssertSuccess { messages ->
                    context.assertFalse(messages.isEmpty())

                    val theMessage = messages.firstOrNull { it.getString("body") == messageBody }
                    context.assertTrue(theMessage != null)

                    val messageAttributes = theMessage?.getJsonObject("messageAttributes")
                    context.assertNotNull(messageAttributes)
                    context.assertEquals(stringAttribute, messageAttributes?.getJsonObject("stringAttribute")?.getString("stringData"))

                    // TODO: uncomment when ElasticMQ supports binary attributes
//                    val receivedByteArray = messageAttributes?.getJsonObject("binaryAttribute")?.getBinary("binaryData")
//                    context.assertTrue(Arrays.equals(binaryAttribute, receivedByteArray))

                    // Delete
                    val receipt = theMessage!!.getString("receiptHandle")
                    context.assertNotNull(receipt)
                    client.deleteMessage(queueName, receipt, context.asyncAssertSuccess() {
                        // Message must be deleted by now, let's check
                        client.receiveMessage(queueName, context.asyncAssertSuccess { messages ->
                            context.assertTrue(messages.firstOrNull { it.getString("body") == messageBody } == null)
                        })
                    })
                })
            })
        }
    }

    private fun TestContext.withClient(clientCode: (SqsClient) -> Unit) {
        val theClient = client
        this.assertNotNull(theClient)
        clientCode(theClient)
    }

}