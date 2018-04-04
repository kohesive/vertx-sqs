package uy.kohesive.vertx.sqs

import io.vertx.core.json.JsonObject

data class SqsMessage(
    val receipt: String,
    val message: JsonObject
)