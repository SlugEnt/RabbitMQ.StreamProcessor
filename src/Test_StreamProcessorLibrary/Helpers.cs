using AutoFixture;
using AutoFixture.AutoMoq;
using RabbitMQ.Stream.Client.Reliable;

namespace Test_StreamProcessorLibrary;

public static class Helpers
{
    /// <summary>
    /// Sends the given number of messages to the producerTst. Returns the last message #
    /// </summary>
    /// <param name="producerTst"></param>
    /// <param name="quantity"></param>
    /// <param name="startingMsgNumber"></param>
    internal static int SendTestMessages(MqTesterProducer producerTst, int quantity, int startingMsgNumber)
    {
        int i;
        int x = startingMsgNumber + quantity;


        for (i = startingMsgNumber; i < x; i++)
        {
            producerTst.SendMessage("Msq #" + i);
        }

        return i;
    }


    internal static (int sent, int confirmed) SendAndConfirmTestMessages(MqTesterProducer producerTst, int quantity, int startingMsgNumber, ConfirmationStatus status)
    {
        int count = SendTestMessages(producerTst, quantity, startingMsgNumber);
        int confirmed = producerTst.TST_ReturnProducerConfirmations(quantity, status);
        return (count, confirmed);
    }

    public static MqTesterProducer SetupProducer(string streamName = "produce", string appName = "a")
    {
        Fixture fixture = new Fixture();
        fixture.Customize(new AutoMoqCustomization());
        MqTesterProducer producerTst = fixture.Create<MqTesterProducer>();
        producerTst.Initialize(streamName, appName);
        return producerTst;
    }


    public static MqTesterConsumer SetupConsumer(string streamName = "produce", string appName = "a")
    {
        Fixture fixture = new Fixture();

        fixture.Customize(new AutoMoqCustomization());
        //MqTesterConsumer consumerTst = fixture.Create<MqTesterConsumer>();
        MqTesterConsumer consumerTst = fixture.Build<MqTesterConsumer>().OmitAutoProperties().Create();
        consumerTst.Initialize(streamName, appName);
        return consumerTst;
    }
}