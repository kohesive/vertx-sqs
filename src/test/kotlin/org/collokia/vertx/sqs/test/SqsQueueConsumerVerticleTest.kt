package org.collokia.vertx.sqs.test

import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import org.collokia.vertx.sqs.SqsClient
import org.elasticmq.rest.sqs.SQSRestServer
import org.elasticmq.rest.sqs.SQSRestServerBuilder
import org.junit.*
import org.junit.runner.RunWith
import org.junit.runners.MethodSorters
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.platform.platformStatic
import kotlin.properties.Delegates

@RunWith(VertxUnitRunner::class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class SqsQueueConsumerVerticleTest {

    companion object {

        val vertx: Vertx = Vertx.vertx()

        val ElasticMqPort = 9324
        val ElasticMqHost = "localhost"

        fun getQueueUrl(queueName: String) = "http://$ElasticMqHost:$ElasticMqPort/queue/$queueName"

        private var client: SqsClient by Delegates.notNull()
        private var sqsServer: SQSRestServer by Delegates.notNull()

        val config = JsonObject(mapOf(
            // SQS client config
            "host"      to ElasticMqHost,
            "port"      to ElasticMqPort,
            "accessKey" to "someAccessKey",
            "secretKey" to "someSecretKey",
            "region"    to "us-west-2",

            // Consumer verticle config
            "pollingInterval" to 1000,
            "queueUrl"        to getQueueUrl("testQueue"),
            "address"         to "sqs.queue.test"
        ))

        @BeforeClass
        @platformStatic
        fun before(context: TestContext) {
            sqsServer = SQSRestServerBuilder.withPort(ElasticMqPort).start()

            println("Started SQS server")

            client = SqsClient.create(vertx, config)
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

    var deploymentId: String = ""

    @Before
    fun beforeTest(context: TestContext) {
        context.withClient { client ->
            client.createQueue("testQueue", mapOf("VisibilityTimeout" to "1"), context.asyncAssertSuccess() { queueUrl ->
                context.assertEquals(queueUrl, getQueueUrl("testQueue"))

                vertx.deployVerticle("org.collokia.vertx.sqs.SqsQueueConsumerVerticle", DeploymentOptions().setConfig(config), context.asyncAssertSuccess() {
                    deploymentId = it
                })
            })
        }
    }

    @After
    fun afterTest(context: TestContext) {
        context.withClient { client ->
            client.deleteQueue(getQueueUrl("testQueue"), context.asyncAssertSuccess())
        }
    }

    @Test
    fun testConsumeWithDeleteAcknowledge(context: TestContext) {
        testConsume(context, true)
    }

    @Test
    fun testConsumeWithoutDeleteAcknowledge(context: TestContext) {
        testConsume(context, false)
    }

    private fun testConsume(context: TestContext, acknowledgeDelete: Boolean) {
        var latch       = CountDownLatch(1)
        val testQueue   = getQueueUrl("testQueue")
        val messageBody = "Test message body, acknowledged=$acknowledgeDelete"

        context.withClient { client ->
            client.sendMessage(testQueue, messageBody, context.asyncAssertSuccess())
        }

        val consumer = vertx.eventBus().consumer("sqs.queue.test", Handler { message: Message<JsonObject> ->
            if (acknowledgeDelete) {
                message.reply(null) // delete the message
            }

            context.assertEquals(messageBody, message.body().getString("body"))
            latch.countDown()
        })

        latch.await(3, TimeUnit.SECONDS)

        // We wait longer than VisibilityTimeout, undeploy all the consumers and check if the queue is empty or not
        vertx.undeploy(deploymentId, context.asyncAssertSuccess() {
            consumer.unregister()

            vertx.executeBlocking(Handler { future: Future<Void> ->
                Thread.sleep(1500)
                future.complete()
            }, context.asyncAssertSuccess() {
                context.withClient { client ->
                    client.receiveMessage(testQueue, context.asyncAssertSuccess() { messages ->
                        println("${ messages.size() } message(s) received: ${ messages.joinToString(", ") }")
                        context.assertEquals(acknowledgeDelete, messages.isEmpty())
                    })
                }
            })
        })
    }

    private fun TestContext.withClient(clientCode: (SqsClient) -> Unit) {
        val theClient = client
        this.assertNotNull(theClient)
        clientCode(theClient)
    }

}