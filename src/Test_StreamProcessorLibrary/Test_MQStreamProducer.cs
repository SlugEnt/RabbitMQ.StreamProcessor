using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using AutoFixture;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.MQStreamProcessor;

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
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit = limit;


        int qtyToSend      = messagesToSend;
        int nextMessageNum = Helpers.SendTestMessages(producerTst, qtyToSend, 1);
        int count          = producerTst.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        Assert.AreEqual(producerTst.CircuitBreakerTripped, circuitBreakerShouldTrip, "A999:  Circuit Breaker test Failed.");
    }



    // Tests that Good Message Confirmations reset the consecutive errors
    [Test]
    public void IntermittentConfirmationFailures()
    {
        // We have to turn off Auto-Retries.  The logic will interfere with the Circuitbreaker tests
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 4;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures
        int qtyToSend = 3;
        (int nextMessageNum, int count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "A20:  Circuit Breaker was tripped. Should not have.");


        // C - Send a good message.  Should reset circuit breaker
        qtyToSend               = 1;
        (nextMessageNum, count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, nextMessageNum, ConfirmationStatus.Confirmed);
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "A100:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(0, producerTst.ConsecutiveErrors, "A110: Should be zero");


        // D - 3 more failures.  Ensure no circuit breaker failure
        qtyToSend               = 3;
        (nextMessageNum, count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, nextMessageNum, ConfirmationStatus.ClientTimeoutError);
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "A200:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(qtyToSend, producerTst.ConsecutiveErrors, "A210: Should be equal to the quantity of messages sent");


        // E - Send one more failure - should trip
        qtyToSend               = 1;
        (nextMessageNum, count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, nextMessageNum, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "A300:  Circuit Breaker was not tripped. Should have.");
    }


    /// <summary>
    /// Confirms failed confirmations go into the retry queue count.
    /// </summary>
    [Test]
    public void RetryQueueCount()
    {
        // We have to turn off Auto-Retries.  The logic will interfere with the Circuitbreaker tests
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 5 messages - All Failures
        int qtyToSend = 5;
        (int nextMessageNum, int count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");
        producerTst.TST_ReturnProducerConfirmations(qtyToSend, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "A20:  Circuit Breaker was not tripped. Should have.");
        Assert.AreEqual(qtyToSend, producerTst.Stat_RetryQueuedMessageCount, "A30:  Retry Queue is not expected value");
    }


    // Test that a successful SendMessageAsync returns true
    [Test]
    public async Task SendMessageTrueIfCircuitBreakerNotSet()
    {
        // We initially turn off Auto-Retries.  We will turn it on after initial set of messages
        MqTesterProducer producerTst = Helpers.SetupProducer();


        // B - Send 1 messages - Failure
        int  qtyToSend = 1;
        bool rc        = await producerTst.SendMessageAsync("Success MSG");
        Assert.IsTrue(rc, "A10:  Successful SendMessageAsync should return true");
    }



    // Test that a SendMessageAsync returns false if the Circuit Breaker is set.
    [Test]
    public async Task SendMessageFailsIfCircuitBreakerSet()
    {
        // We initially turn off Auto-Retries.  We will turn it on after initial set of messages
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 1;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 1 messages - Failure
        int qtyToSend = 1;
        (int nextMessageNum, int count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);
        Assert.AreEqual(qtyToSend, count, "A10: Messages received was not the correct amount.");


        // C - Send another failure.  Should trip Circuit Breaker
        (nextMessageNum, count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "A20:  Circuit Breaker was not tripped. Should have.");

        // D - Try to send another it should return false.
        bool rc = await producerTst.SendMessageAsync("Failed Message");
        Assert.IsFalse(rc, "A30: SendMessageAsync should have returned false");
    }



    [Test]
    public void RetrySendsMessages()
    {
        // We initially turn off Auto-Retries.  We will turn it on after initial set of messages
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures to trip Circuit Breaker
        int qtyToSend = 3;
        (int nextMessageNum, int count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

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
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures to trip Circuit Breaker
        int qtyToSend = 3;
        (int nextMessageNum, int count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);

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
        Assert.AreEqual(qtyToSend, producerTst.Stat_MessagesSuccessfullyConfirmed, "C120:");
    }


    [Test]
    public void CircuitBreakerResetsAfterSuccessfulSend()
    {
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Send 3 messages - All Failures to trip Circuit Breaker
        int qtyToSend = 3;
        (int nextMessageNum, int count) = Helpers.SendAndConfirmTestMessages(producerTst, qtyToSend, 1, ConfirmationStatus.ClientTimeoutError);
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "B10:  Circuit Breaker was not tripped. Should have.");


        // C - Retry to send messages
        producerTst.AutoRetryFailedConfirmations = true;
        producerTst.TST_TurnAutoRetryProcessOn();

        // Sleep to allow the retry thread to do its thing.
        Thread.Sleep(500);

        // Send the command to publish the messages BUT SUCCESSFULLY (Note, this is only a test capability)
        count = producerTst.TST_ReturnProducerConfirmations(-1, ConfirmationStatus.Confirmed);
        Thread.Sleep(100);
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "C100:  Circuit Breaker was still tripped. Should have reset.");
    }


    [Test]
    public void CheckCircuitBreakerResets()
    {
        MqTesterProducer producerTst = Helpers.SetupProducer();
        producerTst.CircuitBreakerStopLimit      = 3;
        producerTst.AutoRetryFailedConfirmations = false;

        // B - Manually set CircuitBreaker
        producerTst.TST_ManuallyTripCircuitBreaker();
        Assert.IsTrue(producerTst.CircuitBreakerTripped, "B10:  Circuit Breaker was not tripped. Should have.");


        // C - Check it, it should reset
        bool result = producerTst.CheckCircuitBreaker();
        Assert.IsFalse(result, "C100 - Should have returned Circuit Breaker Status");
        Assert.IsFalse(producerTst.CircuitBreakerTripped, "C110: Circuit Breaker should not be tripped");
    }
}