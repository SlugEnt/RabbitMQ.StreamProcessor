using RabbitMQ.Stream.Client;
using System.ComponentModel;
using System.Drawing;
using RabbitMQ.Stream.Client.AMQP;
using SlugEnt.StreamProcessor;

namespace StreamProcessor.Console.SampleA;

public class SampleA
{
    private const string AP_BATCH = "Batch";

    private string _streamName = "Sample.A1";
    private SampleA_Producer _producerA;
    private string _batch;
    private int _messagesInBatch;

    public SampleA(int messagesInBatch = 3)
    {
        // Build a producer
        _producerA = new SampleA_Producer(_streamName, "Sample.A.Producer", ProduceMessages);
        _producerA.SetStreamLimitsRaw(1000, 100, 900);
//        _producerA.SetStreamLimits(3, 1, 36);
        _messagesInBatch = messagesInBatch;
        _producerA.MessageConfirmationError += MessageConfirmationError;
        _producerA.MessageConfirmationSuccess += MessageConfirmationSuccess;
    }


    public async Task Start()
    {
        _producerA.Start();

        bool keepProcessingB = true;
        while (keepProcessingB)
        {
            if (System.Console.KeyAvailable)
            {
                ConsoleKeyInfo d1KeyInfo = System.Console.ReadKey();
                if (d1KeyInfo.Key == ConsoleKey.X)
                {
                    keepProcessingB = false;
                    await _producerA.Stop();
                }
            }

            System.Console.WriteLine($"Queued Messages: {_producerA.Stat_RetryQueuedMessageCount}");
            System.Console.WriteLine($"Message Counter: {_producerA.MessageCounter}");
            System.Console.WriteLine($"Circuit Breaker Status: {_producerA.CircuitBreakerTripped}");

            //streamB.CheckStatus();
            Thread.Sleep(3000);

        }

        System.Console.WriteLine($"Stream B has completed all processing.");
    }



    protected async Task ProduceMessages(SampleA_Producer producer, BackgroundWorker backgroundWorker)
    {
        _batch = "A";
        while (!backgroundWorker.CancellationPending)
        {
            // Publish the messages
            for (var i = 0; i <882; i++)
            {
                string msg = String.Format($"ID: {i} hello");

                Message message = producer.CreateMessage(msg);
                string fullBatch = _batch.ToString() + i.ToString();
                message.ApplicationProperties.Add(AP_BATCH, fullBatch);
                message.Properties.ReplyTo = "scott";
                
                if (!producer.CircuitBreakerTripped) 
                    await producer.SendMessage(message);
                else
                {
                    bool keepTrying = true;
                    while (keepTrying)
                    {
                        if (producer.CircuitBreakerTripped) Thread.Sleep(2000);
                        else await producer.SendMessage(message);
                    }
                }
            }

            _batch = HelperFunctions.NextBatch(_batch);

            Thread.Sleep(2000);
        }
    }

    private void MessageConfirmationError(object sender, MessageConfirmationEventArgs e)
    {
        string batch = GetBatch(e);
        HelperFunctions.WriteInColor($"ConfErr:  Msg Batch:  {batch} ", ConsoleColor.Red);
    }

    private void MessageConfirmationSuccess(object sender, MessageConfirmationEventArgs e)
    {
        string batch = GetBatch(e);
        HelperFunctions.WriteInColor($"Success:  MSg Batch:  {batch}", ConsoleColor.Green);
    }

    private string GetBatch(MessageConfirmationEventArgs e)
    {
        string batch = "";
        if (e.Messages[0].ApplicationProperties.ContainsKey(AP_BATCH))
        {
            batch = (string)e.Messages[0].ApplicationProperties[AP_BATCH];
        }
        else batch = "Not Specified";

        return batch;
    }
}
