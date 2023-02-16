using AutoFixture;
using AutoFixture.NUnit3;
using NUnit.Framework.Internal;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Test_StreamProcessorLibrary;


[TestFixture]
public class Test_MQStreamerConsumer
{

    [Test]
    public async Task DecodeMessage()
    {
        string messageText = "Hello";
        MqTesterProducer producerTst = Helpers.SetupProducer();

        Message newMessage = producerTst.CreateMessage(messageText);

        // Decode Mesage
        MqTesterConsumer consumerTst = Helpers.SetupConsumer();
        string value = consumerTst.DecodeMessage(newMessage);
        Assert.AreEqual(messageText,value,"A10:");

    }


    [Test]
    public async Task CheckPointSetsDateTime()
    {
        // Build a consumer
        MqTesterConsumer consumerTst = Helpers.SetupConsumer();

        
        // B - Pretend we Consumed
        DateTime currentDateTime = DateTime.Now;
        Assert.LessOrEqual(consumerTst.CheckpointLastDateTime,currentDateTime,"B120:");


        // Manual Set of Check Point 
        await consumerTst.CheckPointSet();
        Assert.AreEqual(0,consumerTst.CheckpointOffsetCounter, "C200:");
        Assert.Greater(consumerTst.CheckpointLastDateTime,currentDateTime,"C210:");
    }



    [Test]
    public async Task CheckPointSetsLastOffset()
    {
        // Build a producer and consumer
        MqTesterProducer producerTst = Helpers.SetupProducer();
        MqTesterConsumer consumerTst = Helpers.SetupConsumer();
        consumerTst.ProducerMessageQueue = producerTst.MessageQueue;

        // First build 3 messages
        int qtyToSend = 3;
        int nextMessageNum = Helpers.SendTestMessages(producerTst, qtyToSend, 1);


        // B - Consume the messages 
        DateTime currentDateTime = DateTime.Now;
        consumerTst.TST_ConsumeMessages(qtyToSend);
        Assert.AreEqual(qtyToSend, consumerTst.ConsumerMessageQueue.Count, "B100: Messages Queue Incorrect.");
        Assert.AreEqual(qtyToSend, consumerTst.CheckpointOffsetCounter, "B110: ");
        Assert.LessOrEqual(consumerTst.CheckpointLastDateTime, currentDateTime, "B120:");


        // Manual Set of Check Point 
        await consumerTst.CheckPointSet();
        Assert.AreEqual(0, consumerTst.CheckpointOffsetCounter, "C200:");
        Assert.Greater(consumerTst.CheckpointLastDateTime, currentDateTime, "C210:");
        Assert.AreEqual(qtyToSend, consumerTst.CheckpointLastOffset, "C220:");
    }


}