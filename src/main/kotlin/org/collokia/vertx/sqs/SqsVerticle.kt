package org.collokia.vertx.sqs

import com.amazonaws.auth.AWSCredentialsProvider
import io.vertx.core.logging.Logger

interface SqsVerticle {

    val log: Logger
    val client: SqsClient
    var credentialsProvider: AWSCredentialsProvider?

    fun deleteMessage(queueUrl: String, reciept: String) {
        client.deleteMessage(queueUrl, reciept) {
            if (it.failed()) {
                log.warn("Unable to acknowledge message deletion with receipt = $reciept")
            }
        }
    }

}