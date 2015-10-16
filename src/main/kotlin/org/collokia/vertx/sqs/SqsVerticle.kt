package org.collokia.vertx.sqs

import io.vertx.core.logging.Logger

interface SqsVerticle {

    val log: Logger

    val client: SqsClient

    fun deleteMessage(queueUrl: String, reciept: String) {
        client.deleteMessage(queueUrl, reciept) {
            if (it.failed()) {
                log.warn("Unable to acknowledge message deletion with receipt = $reciept")
            }
        }
    }

}