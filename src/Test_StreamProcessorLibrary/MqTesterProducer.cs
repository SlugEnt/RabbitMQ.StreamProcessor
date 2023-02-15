using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;
using Microsoft.Extensions.Logging;

namespace Test_StreamProcessorLibrary;


/// <summary>
/// This class is derived from the MqStreamProducer.  It is used to enable the unit testing of some of the RabbitMQ functionality.
/// For instance, SendingMessages actually just sends to a queue.
/// Also some classes are unable to be instantiated outside the RabbitMQ Streams client.  This bypasses that requirement.
/// </summary>
public class MqTesterProducer : MqStreamProducer
{
    // Simulating MQ
    private Queue<Message> _messagesProduced = new Queue<Message>();


    /// <summary>
    /// Simulates a MQ Stream instance.  Can publish, publish confirm and consume messages.
    /// </summary>
    /// <param name="streamName"></param>
    /// <param name="appName"></param>
    public MqTesterProducer(ILogger<MqTesterProducer> logger) : base(logger)
    {
        // Automatically assume we are connected.
        IsConnected = true;
    }


    /// <summary>
    /// This is needed to allow the Test Consumer to get the messages that are produced.
    /// </summary>
    public Queue<Message> MessageQueue
    {
        get { return _messagesProduced; }
    }

    /// <summary>
    /// Turns the auto retry process thread on
    /// </summary>

    public void TST_TurnAutoRetryProcessOn()
    {
        TurnAutoRetryThreadOn();
    }


    /// <summary>
    /// Allows caller to manually trip the circuit breaker
    /// </summary>
    public void TST_ManuallyTripCircuitBreaker()
    {
        CircuitBreakerTripped = true;
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
