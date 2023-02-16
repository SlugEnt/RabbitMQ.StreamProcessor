# RabbitMQ.StreamProcessor
High Level library for interacting with RabbitMQ streams using the base library --> [rabbitmq-stream-dotnet-client](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client)

This library provides a higher level interface for working with RabbitMQ Streams that makes its use in applications quick and very easy.

It supports a number of features out of the box including:
* Ability to automatically perform Offset Storage in the MQ Stream for the specific application after a given number of messages have been received.
* Provide statistics on number of messages received / produced / successful / failures
* Built in Circuit Breaker that stops sending messages after a given number of consecutive failures
* Automatic retry logic for failed publishes
* Auto-retry can be turned off
* Retry logic if successful resets circuit breaker allowing full resumption of publishing messages
* Optional subscription to events:
    * Publish Confirmation
    * Publish Failure
    * CheckPoint Saved (Offset set in stream for the given application)
* Ability to read last offsets for the given stream / application
* Ability to call CircuitBreak and ask it to recheck its status
* Can Send simple Messages (Text) only
* Can Send complex Messages
    * Creation Time automatically set
    * The application that generated the message is stored in message
    * Can add any number of additional ApplicationProperties to the message with having to store them in the body
* Ability to define stream on start if it does not exist yet.
* Ability to configure new stream, per:
    * Maximum Size
    * Maximum Segment Size
    * Max Age
* Ability to set stream size / age limits in sizes such as MB or KB instead of bytes*
* Built in Logging
* Multi-Threaded


