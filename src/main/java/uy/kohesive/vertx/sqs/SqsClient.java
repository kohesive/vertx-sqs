package uy.kohesive.vertx.sqs;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import kotlin.Unit;
import uy.kohesive.vertx.sqs.impl.SqsClientImpl;

import java.util.List;
import java.util.Map;

@VertxGen
public interface SqsClient {

    static SqsClient create(Vertx vertx, JsonObject config) {
        return new SqsClientImpl(vertx, config, null);
    }

    /**
     * Async result st a queue's URL.
     */
    void createQueue(String name, Map<String, String> attributes, Handler<AsyncResult<String>> resultHandler);

    void deleteQueue(String queueUrl, Handler<AsyncResult<Unit>> resultHandler);

    /**
     * Async result is a list of queues' URLs. 'namePrefix' is nullable.
     */
    void listQueues(String namePrefix, Handler<AsyncResult<List<String>>> resultHandler);

    /**
     * Async result is a message's Id.
     *
     * Message attributes JSON format:
     * <code>
     *   {
     *     "someLabel":{
     *       "dataType":"String",
     *       "stringData":"Hello World"
     *     },
     *     "anotherLabel":{
     *       "dataType":"Binary",
     *       "stringData":"TWFuIGlzIGRpc3Rpbmd1"
     *     }
     *   }
     * </code>
     *
     */
    // TODO: add support for attributes
    void sendMessage(String queueUrl, String messageBody, JsonObject attributes, Integer delaySeconds, Handler<AsyncResult<String>> resultHandler);

    /**
     * Async result is a message's Id.
     */
    // TODO: add support for attributes
    void sendMessage(String queueUrl, String messageBody, JsonObject attributes, Handler<AsyncResult<String>> resultHandler);

    /**
     * Async result is a message's Id.
     */
    // TODO: add support for attributes
    void sendMessage(String queueUrl, String messageBody, Handler<AsyncResult<String>> resultHandler);

    /**
     * Async result is a message JSON object.
     */
    void receiveMessage(String queueUrl, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    void receiveMessages(String queueUrl, Integer maxMessages, Handler<AsyncResult<List<JsonObject>>> resultHandler);

    void deleteMessage(String queueUrl, String receiptHandle, Handler<AsyncResult<Unit>> resultHandler);

    void setQueueAttributes(String queueUrl, Map<String, String> attributes, Handler<AsyncResult<Unit>> resultHandler);

    void changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout, Handler<AsyncResult<Unit>> resultHandler);

    /**
     * Async result is the queue's URL. 'queueOwnerAWSAccountId' is nullable.
     */
    void getQueueUrl(String queueName, String queueOwnerAWSAccountId, Handler<AsyncResult<String>> resultHandler);

    void addPermissionAsync(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions, Handler<AsyncResult<Unit>> resultHandler);

    void removePermission(String queueUrl, String label, Handler<AsyncResult<Unit>> resultHandler);

    /**
     * Async result is the attributes' keys/values map. 'attributeNames' is nullable.
     */
    void getQueueAttributes(String queueUrl, List<String> attributeNames, Handler<AsyncResult<JsonObject>> resultHandler);

//    void purgeQueue(String queueUrl, Handler<AsyncResult<Unit>> resultHandler);

    void listDeadLetterSourceQueues(String queueUrl, Handler<AsyncResult<List<String>>> resultHandler);

    void start(Handler<AsyncResult<Unit>> resultHandler);

    void stop(Handler<AsyncResult<Unit>> resultHandler);

}
