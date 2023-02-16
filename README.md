# RabbitMQ.StreamProcessor

![Build Test Event](https://github.com/slugent/RabbitMQ.StreamProcessor/actions/workflows/buildtest.yml/badge.svg?event=push)

High Level library for interacting with RabbitMQ streams using the base library --> [rabbitmq-stream-dotnet-client](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client)

This library provides a higher level interface for working with RabbitMQ Streams that makes its use in applications quick and very easy.

# Features
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



# Sample Use
```
    string _streamName = "Sample.B";
    ISampleB_Producer _producerB;
    
    StreamSystemConfig config = new StreamSystemConfig();

    _producerB = _serviceProvider.GetService<ISampleB_Producer>();

    // Initialize the Stream.  We will access _streamName and the Application name is "producerB"
    _producerB.Initialize(_streamName,"producerB",config);

    // Set the method that generates Messages
    _producerB.SetProducerMethod(ProduceMessages);

    // Create a stream that has max size of 1000 bytes, max segment size of 100 bytes and max age of 900 seconds
    _producerB.SetStreamLimitsRaw(1000, 100, 900);

    // Define Event handlers.  This is optional
    _producerB.MessageConfirmationError += MessageConfirmationError;
    _producerB.MessageConfirmationSuccess += MessageConfirmationSuccess;

    // Start producing messages
    await _producerB.Start();


    // Build a consumer
    _consumerB_slow = _serviceProvider.GetService<ISampleB_Consumer>();
    _consumerB_slow.Initialize(_streamName, "consumer_slow",config);
    _consumerB_slow.SetConsumptionHandler(ConsumeSlow);

    // Optional if you want to know when a Checkpoint has occurred
    _consumerB_slow.EventCheckPointSaved += OnEventCheckPointSavedSlow;


    // Start Consuming messages
    await _consumerB_slow.Start();


    // Produce Messages Method
    protected async Task ProduceMessages(SampleB_Producer producer)
    {
        Message message = producer.CreateMessage("hello");
        await producer.SendMessage(message);
    }


    // Consume Messages method
    private async Task<bool> ConsumeSlow(Message message)
    {
        // Do something with Message
        string msg = Encoding.Default.GetString(message.Data.Contents.ToArray());
        Console.WriteLine("msg: ", msg);

        // Simulate slow
        Thread.Sleep(1500);
        return true;
    }
```