using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;

namespace Test_StreamProcessorLibrary;




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
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");
        producerTst.CircuitBreakerStopLimit = limit;


        int qtyToSend = messagesToSend;
        int nextMessageNum = SendTestMessages(producerTst, qtyToSend, 1);
        int count = producerTst.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        Assert.AreEqual(producerTst.CircuitBreakerTripped, circuitBreakerShouldTrip, "A999:  Circuit Breaker test Failed.");

    }



    // Tests that Good Message Confirmations reset the consecutive errors
    [Test]
    public void IntermittentConfirmationFailures()
    {
        // We have to turn off Auto-Retries.  The logic will interfere with the Circuitbreaker tests
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");
        producerTst.CircuitBreakerStopLimit = 4;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures
        int qtyToSend = 3;
        (int nextMessageNum, int count) = SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "A20:  Circuit Breaker was tripped. Should not have.");


        // C - Send a good message.  Should reset circuit breaker
        qtyToSend = 1;
        (nextMessageNum, count) = SendAndConfirmTestMessages(producerTst, qtyToSend, nextMessageNum, ConfirmationStatus.Confirmed);
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "A100:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(0,producerTst.ConsecutiveErrors,"A110: Should be zero");


        // D - 3 more failures.  Ensure no circuit breaker failure
        qtyToSend = 3;
        (nextMessageNum, count) = SendAndConfirmTestMessages(producerTst, qtyToSend, nextMessageNum, ConfirmationStatus.ClientTimeoutError);
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "A200:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(qtyToSend, producerTst.ConsecutiveErrors, "A210: Should be equal to the quantity of messages sent");


        // E - Send one more failure - should trip
        qtyToSend = 1;
        (nextMessageNum, count) = SendAndConfirmTestMessages(producerTst, qtyToSend, nextMessageNum, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "A300:  Circuit Breaker was not tripped. Should have.");
    }


    /// <summary>
    /// Confirms failed confirmations go into the retry queue count.
    /// </summary>
    [Test]
    public void RetryQueueCount()
    {
        // We have to turn off Auto-Retries.  The logic will interfere with the Circuitbreaker tests
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");
        producerTst.CircuitBreakerStopLimit = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 5 messages - All Failures
        int qtyToSend = 5;
        (int nextMessageNum, int count) = SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        producerTst.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "A20:  Circuit Breaker was not tripped. Should have.");
        Assert.AreEqual(qtyToSend,producerTst.Stat_RetryQueuedMessageCount,"A30:  Retry Queue is not expected value");
    }


    // Test that a successful SendMessage returns true
    [Test]
    public async Task SendMessageTrueIfCircuitBreakerNotSet()
    {
        // We initially turn off Auto-Retries.  We will turn it on after initial set of messages
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");


        // B - Send 1 messages - Failure
        int qtyToSend = 1;
        bool rc = await producerTst.SendMessage("Success MSG");
        Assert.IsTrue(rc,"A10:  Successful SendMessage should return true");
    }



    // Test that a SendMessage returns false if the Circuit Breaker is set.
    [Test]
    public async Task SendMessageFailsIfCircuitBreakerSet()
    {
        // We initially turn off Auto-Retries.  We will turn it on after initial set of messages
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");
        producerTst.CircuitBreakerStopLimit = 1;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 1 messages - Failure
        int qtyToSend = 1;
        (int nextMessageNum, int count) = SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);
        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");


        // C - Send another failure.  Should trip Circuit Breaker
        (nextMessageNum, count) = SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "A20:  Circuit Breaker was not tripped. Should have.");

        // D - Try to send another it should return false.
        bool rc = await producerTst.SendMessage("Failed Message");
        Assert.IsFalse(rc,"A30: SendMessage should have returned false");
    }



    [Test]
    public void RetrySendsMessages()
    {
        // We initially turn off Auto-Retries.  We will turn it on after initial set of messages
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");
        producerTst.CircuitBreakerStopLimit = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures to trip Circuit Breaker
        int qtyToSend = 3;
        (int nextMessageNum, int count) = SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "B10: Messages received was not the correct amount.");
        Assert.AreEqual(qtyToSend, producerTst.Stat_RetryQueuedMessageCount, "B20:  Retry Queue is not expected value");
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "C30:  Circuit Breaker was not tripped. Should have.");


        // C - Turn AutoRetry On.
        // NOTE:  DO NOT STOP TO DEBUG IN THIS Step C - Or else some of the results will fail.
        producerTst.AutoRetryFailedConfirmations = true;
        producerTst.TST_TurnAutoRetryProcessOn();

        // Sleep to allow the retry thread to do its thing.
        Thread.Sleep(MqStreamProducer.CIRCUIT_BREAKER_MIN_SLEEP + 100);

        // There now should be zero messages in the Retry Queue.
        Assert.AreEqual(0, producerTst.Stat_RetryQueuedMessageCount, "C100:  Retry Queue is not empty");

        // Send the command to publish the messages (Note, this is only a test capability)
        count = producerTst.TST_ReturnProducerConfirmations(-1, ConfirmationStatus.ClientTimeoutError);
        
        Assert.AreEqual(count, producerTst.Stat_RetryQueuedMessageCount, "C110:  Retry Queue is not expected value");
    }


    [Test]
    public void RetrySendsMessagesSuccess()
    {
        MQStreamProducer_TST producerTst = new MQStreamProducer_TST("abc", "a1");
        producerTst.CircuitBreakerStopLimit = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures to trip Circuit Breaker
        int qtyToSend = 3;
        (int nextMessageNum, int count) = SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "B10: Messages received was not the correct amount.");
        Assert.AreEqual(qtyToSend, producerTst.Stat_RetryQueuedMessageCount, "B20:  Retry Queue is not expected value");
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "C30:  Circuit Breaker was not tripped. Should have.");


        // C - Turn AutoRetry On.
        // NOTE:  DO NOT STOP TO DEBUG IN THIS Step C - Or else some of the results will fail.
        producerTst.AutoRetryFailedConfirmations = true;
        producerTst.TST_TurnAutoRetryProcessOn();

        // Sleep to allow the retry thread to do its thing.
        Thread.Sleep(500);

        // Send the command to publish the messages BUT SUCCESSFULLY (Note, this is only a test capability)
        count = producerTst.TST_ReturnProducerConfirmations(-1, ConfirmationStatus.Confirmed);
        Thread.Sleep(100);
        Assert.AreEqual(0, producerTst.Stat_RetryQueuedMessageCount, "C110:  Retry Queue is not expected value");
        Assert.AreEqual(qtyToSend,producerTst.Stat_MessagesSuccessfullyConfirmed,"C120:");
    }




    /// <summary>
    /// Sends the given number of messages to the producerTst. Returns the last message #
    /// </summary>
    /// <param name="producerTst"></param>
    /// <param name="quantity"></param>
    /// <param name="startingMsgNumber"></param>
    private int SendTestMessages(MQStreamProducer_TST producerTst, int quantity, int startingMsgNumber)
    {
        int i;
        int x = startingMsgNumber + quantity;


        for (i = startingMsgNumber; i < x; i++)
        {
            producerTst.SendMessage("Msq #" + i);
        }

        return i;
    }


    private (int sent, int confirmed) SendAndConfirmTestMessages(MQStreamProducer_TST producerTst, int quantity, int startingMsgNumber, ConfirmationStatus status)
    {
        int count = SendTestMessages(producerTst, quantity, startingMsgNumber);
        int confirmed = producerTst.TST_ReturnProducerConfirmations(quantity, status);
        return (count,confirmed);
    }
}


