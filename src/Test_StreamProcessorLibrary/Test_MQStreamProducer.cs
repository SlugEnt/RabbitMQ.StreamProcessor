using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;

namespace Test_StreamProcessorLibrary;


public class TestMqStreamProducer : MqStreamProducer
{
    // Simulating MQ
    private Queue<Message> _messagesProduced = new Queue<Message>();


    public TestMqStreamProducer(string streamName, string appName) : base(streamName, appName)
    {
        // Automatically assume we are connected.
        IsConnected = true;
    }

    // When true, messages are returned as successfully sent to producer.  False, the have ConfirmationTimeouts
    public bool MessageConfirmationSuccess { get; set; }


    public void TST_ImitateErroredMessage()
    {
        ConsecutiveErrors++;
    }

    public void TST_ImitateSuccessMessage()
    {
        ConsecutiveErrors = 0;
    }
    

    // Create an ovverride for the SendMessageToMQ so we do not need a running MQ instance.
    protected override async Task SendMessageToMQ(Message message)
    {
        _messagesProduced.Enqueue(message);
    }


    /// <summary>
    /// Returns the given number of messages out of the messagesProduced queue with the confirmation as successful or in error
    /// </summary>
    /// <param name="qtyToDequeu">How many messages to retrieve</param>
    /// <param name="AsSuccessful">Whether they should be successful or failure confirmations</param>
    public short TST_ReturnProducerConfirmations(int qtyToDequeu = 1, ConfirmationStatus statusToBeReturned = ConfirmationStatus.Confirmed)
    {
        List<Message> messages = new List<Message>();

        short i;
        for ( i = 0; i < qtyToDequeu; i++)
        {
            if (_messagesProduced.TryDequeue(out Message message))
            {
                messages.Add(message);
            }
        }
        
        // Call the Confirmation Processor
        ConfirmationProcessor(statusToBeReturned,messages);
        return i;
    }
}



[TestFixture]
public class Test_MQStreamProducer
{
    bool confirmed_A = false;



    // Test the Circuit Breaker Tripped Flag
    [Test]
    [TestCase(3, 1, false)]
    [TestCase(3, 4, true)]
    [TestCase(3, 3, true)]
    public void CircuitBreakerTrips(int limit, int messagesToSend, bool circuitBreakerShouldTrip)
    {
        TestMqStreamProducer producer = new TestMqStreamProducer("abc", "a1");
        producer.CircuitBreakerStopLimit = limit;


        int qtyToSend = messagesToSend;
        int nextMessageNum = SendTestMessages(producer, qtyToSend, 1);
        int count = producer.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        Assert.AreEqual(producer.CircuitBreakerTripped, circuitBreakerShouldTrip, "A999:  Circuit Breaker test Failed.");

    }



    // Tests that Good Message Confirmations reset the consecutive errors
    [Test]
    public void IntermittentConfirmationFailures()
    {
        // We have to turn off Auto-Retries.  The logic will interfere with the Circuitbreaker tests
        TestMqStreamProducer producer = new TestMqStreamProducer("abc", "a1");
        producer.CircuitBreakerStopLimit = 4;
        producer.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures
        int qtyToSend = 3;
        int nextMessageNum = SendTestMessages(producer,qtyToSend,1);
        int count = producer.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);
        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        producer.TST_ReturnProducerConfirmations(qtyToSend,ConfirmationStatus.ClientTimeoutError);
        Assert.IsFalse(producer.CircuitBreakerTripped, "A20:  Circuit Breaker was tripped. Should not have.");


        // C - Send a good message.  Should reset circuit breaker
        qtyToSend = 1;
        nextMessageNum = SendTestMessages(producer, qtyToSend, nextMessageNum);
        producer.TST_ReturnProducerConfirmations(qtyToSend);
        Assert.IsFalse(producer.CircuitBreakerTripped, "A100:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(0,producer.ConsecutiveErrors,"A110: Should be zero");


        // D - 3 more failures.  Ensure no circuit breaker failure
        qtyToSend = 3;
        nextMessageNum = SendTestMessages(producer,qtyToSend,nextMessageNum);
        producer.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);
        Assert.IsFalse(producer.CircuitBreakerTripped, "A200:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(qtyToSend, producer.ConsecutiveErrors, "A210: Should be equal to the quantity of messages sent");


        // E - Send one more failure - should trip
        qtyToSend = 1;
        nextMessageNum = SendTestMessages(producer, qtyToSend, nextMessageNum);
        producer.TST_ReturnProducerConfirmations(qtyToSend,ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producer.CircuitBreakerTripped, "A300:  Circuit Breaker was not tripped. Should have.");
    }


    /// <summary>
    /// Sends the given number of messages to the producer. Returns the last message #
    /// </summary>
    /// <param name="producer"></param>
    /// <param name="quantity"></param>
    /// <param name="startingMsgNumber"></param>
    private int SendTestMessages(TestMqStreamProducer producer, int quantity, int startingMsgNumber)
    {
        int i;
        int x = startingMsgNumber + quantity;


        for (i = startingMsgNumber; i < x; i++)
        {
            producer.SendMessage("Msq #" + i);
        }

        return i;
    }
}


