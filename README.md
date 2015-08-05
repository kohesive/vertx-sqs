
# Amazon SQS Client for Vert.x

This Vert.x client allows Amazon SQS access in two ways:

* As a @VertxGen service bridge to Amazon SQS Async Client methods
* As an Amazon SQS queue consuming verticle

## Service usage

Client must be configured with a region. It can also be configured with AWS credentials, otherwise a default `~/.aws/credentials` credentials file will be used:

```
JsonObject config = new JsonObject()
    .put("accessKey", "someAccessKey")
    .put("secretKey", "someSecretKey")
    .put("region", "us-west-2");
```

The client is initialized asynchronously:

```
SqsClient client = SqsClient.create(vertx, config);
client.start(result -> {
    if (result.succeeded()) {
        System.out.println("Client is initialized"); 
    }
});
```

Once the client is initialized, it can be used to access the Amazon SQS API in async manner:

```
client.sendMessage("MyQueue", "Hello World", result -> {
    if (result.succeeded()) {
        System.out.println("Message is sent");
    }
});
```

## Queue consuming verticle usage
