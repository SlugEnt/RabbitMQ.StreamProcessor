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
    public TestMqStreamProducer(string streamName, string appName) : base(streamName, appName)
    {
    }

    public void TST_ImitateErroredMessage()
    {
        ConsecutiveErrors++;
    }

    public void TST_ImitateSuccessMessage()
    {
        ConsecutiveErrors = 0;
    }
    
}



[TestFixture]
public class Test_MQStreamProducer
{
    bool confirmed_A = false;


    // Test the Circuit Breaker Tripped Flag
    [Test]
    [TestCase(3,1,false)]
    [TestCase(3, 4, true)]
    [TestCase(3, 3, true)]
    public void CircuitBreakerTrips(int limit, int messagesToSend, bool circuitBreakerShouldTrip)
    {
        TestMqStreamProducer producer = new TestMqStreamProducer("abc", "a1");
        producer.CircuitBreakerStopLimit = limit;
        for (int i = 0; i < messagesToSend; i++)
        {
            producer.TST_ImitateErroredMessage();
        }

        Assert.AreEqual(producer.CircuitBreakerTripped, circuitBreakerShouldTrip,"A10:  Circuit Breaker test Failed.");

    }


    [Test]
    public void IntermittentConfirmationFailures()
    {
        TestMqStreamProducer producer = new TestMqStreamProducer("abc", "a1");
        producer.CircuitBreakerStopLimit = 4;
        for (int i = 0; i < 3; i++)
        {
            producer.TST_ImitateErroredMessage();
        }
        Assert.IsFalse(producer.CircuitBreakerTripped, "A10:  Circuit Breaker was tripped. Should not have.");

        // Send a good message.
        producer.TST_ImitateSuccessMessage();
        Assert.IsFalse(producer.CircuitBreakerTripped, "A20:  Circuit Breaker was tripped. Should not have.");
        Assert.AreEqual(0,producer.ConsecutiveErrors,"A30: Should be zero");

        for (int j = 0; j < 3; j++)
        {
            producer.TST_ImitateErroredMessage();
        }
        Assert.IsFalse(producer.CircuitBreakerTripped, "A40:  Circuit Breaker was tripped. Should not have.");

        // Send one more failure - should trip
        producer.TST_ImitateErroredMessage();
        Assert.IsTrue(producer.CircuitBreakerTripped, "A50:  Circuit Breaker was not tripped. Should have.");
    }

}


