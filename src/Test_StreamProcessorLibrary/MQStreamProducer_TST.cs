using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;

namespace Test_StreamProcessorLibrary;


/// <summary>
/// This class is derived from the MqStreamProducer.  It is used to enable the unit testing of some of the RabbitMQ functionality.
/// For instance, SendingMessages actually just sends to a queue.
/// Also some classes are unable to be instantiated outside the RabbitMQ Streams client.  This bypasses that requirement.
/// </summary>
public class MQStreamProducer_TST : MqStreamProducer
{
    // Simulating MQ
    private Queue<Message> _messagesProduced = new Queue<Message>();


    public MQStreamProducer_TST(string streamName, string appName) : base(streamName, appName)
    {
        // Automatically assume we are connected.
        IsConnected = true;
    }


    /// <summary>
    /// Turns the auto retry process thread on
    /// </summary>

    public void TST_TurnAutoRetryProcessOn()
    {
        TurnAutoRetryThreadOn();
    }
    

    // Create an ovverride for the SendMessageToMQ so we do not need a running MQ instance.
    protected override async Task SendMessageToMQ(Message message)
    {
        _messagesProduced.Enqueue(message);
    }


    /// <summary>
    /// Returns the given number of messages out of the messagesProduced queue with the confirmation as successful or in error
    /// </summary>
    /// <param name="qtyToDequeu">How many messages to retrieve.  If set to -1 then dequeu until empty</param>
    /// <param name="AsSuccessful">Whether they should be successful or failure confirmations</param>
    public short TST_ReturnProducerConfirmations(int qtyToDequeu = 1, ConfirmationStatus statusToBeReturned = ConfirmationStatus.Confirmed)
    {
        List<Message> messages = new List<Message>();

        bool continueToDequeu = true;
        short counter = 0;
        while (continueToDequeu)
        {
            if (_messagesProduced.TryDequeue(out Message message))
            {
                messages.Add(message);
            }
            else
            {
                continueToDequeu = false;
                continue;
            }

            counter++;
            if (qtyToDequeu > -1)
                if (counter == qtyToDequeu)
                    continueToDequeu = false;
        }

        // Call the Confirmation Processor
        ConfirmationProcessor(statusToBeReturned, messages);
        return counter;
    }
}
