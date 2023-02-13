﻿using RabbitMQ.Stream.Client;
using System.ComponentModel;
using System.Drawing;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using StreamProcessor.Console;
using StreamProcessor.Console.SampleB;


namespace StreamProcessor.ConsoleScr.SampleB;


public class SampleB
{
    // Just the key that is added to all messages 
    private const string AP_BATCH = "Batch";

    private string _streamName = "Sample.B";
    private SampleB_Producer _producerB;
    private string _batch;
    private int _messagesInBatch;
    private short _consoleStatsStartLine = 4;

    private short statsListSize = 10;
    private List<Stats> _statsList;


    public SampleB(int messagesInBatch = 3)
    {
        // Build a producer
        _producerB = new SampleB_Producer(_streamName, "Sample.B.Producer", ProduceMessages);
        _producerB.SetStreamLimitsRaw(1000, 100, 900);
        //        _producerB.SetStreamLimits(3, 1, 36);
        _messagesInBatch = messagesInBatch;
        _producerB.MessageConfirmationError += MessageConfirmationError;
        _producerB.MessageConfirmationSuccess += MessageConfirmationSuccess;

        BuildStatsObjects();
        


        DisplayStats = new DisplayStats(_statsList);
        
        DisplayStats.Refresh();
    }



    private void BuildStatsObjects()
    {
        _statsList = new List<Stats>();
        Stats producerStats = new Stats("Producer");
        Stats consumerStatsA = new Stats("ConsA");
        Stats consumerStatsB = new Stats("ConsB");
        Stats consumerStatsC = new Stats("ConsC");
        Stats consumerStatsD = new Stats("ConsD");

        _statsList.Add(producerStats);
        _statsList.Add(consumerStatsA);
        _statsList.Add(consumerStatsB);
        _statsList.Add(consumerStatsC);
        _statsList.Add(consumerStatsD);
    }


    public DisplayStats DisplayStats { get; private set; }


    /// <summary>
    /// The number of messages that should be produced per batch
    /// </summary>
    public short MessagesPerBatch { get; set; } = 6;


    public int IntervalInSecondsBetweenBatches { get; set; } = 30;


    public async Task Start()
    {
        _producerB.Start();

        bool keepProcessingB = true;
        while (keepProcessingB)
        {
            if (System.Console.KeyAvailable)
            {
                ConsoleKeyInfo d1KeyInfo = System.Console.ReadKey();
                if (d1KeyInfo.Key == ConsoleKey.X)
                {
                    keepProcessingB = false;
                    await _producerB.Stop();
                }
            }

            System.Console.WriteLine($"Queued Messages: {_producerB.Stat_RetryQueuedMessageCount}");
            System.Console.WriteLine($"Message Counter: {_producerB.MessageCounter}");
            System.Console.WriteLine($"Circuit Breaker Status: {_producerB.CircuitBreakerTripped}");

            UpdateStats();
            DisplayStats.Refresh();
            Thread.Sleep(3000);

        }

        System.Console.WriteLine($"Stream B has completed all processing.");
    }

    private void UpdateStats()
    {
        _statsList[0].SuccessMessages = _producerB.Stat_MessagesSuccessfullyConfirmed;
        _statsList[0].FailureMessages = _producerB.Stat_MessagesErrored;
        _statsList[0].CircuitBreakerTripped = _producerB.CircuitBreakerTripped;
        
    }


    /// <summary>
    /// Produces Messages
    /// </summary>
    /// <param name="producer"></param>
    /// <param name="backgroundWorker"></param>
    /// <returns></returns>
    protected async Task ProduceMessages(SampleB_Producer producer)
    {
        // Initiate the Batch Number
        _batch = "A";
        while (!producer.IsCancelled)
        {
            try
            {
                // Publish the messages
                for (short i = 0; i < MessagesPerBatch; i++)
                {
                    string fullBatchId = _batch + i;
                    string msg = String.Format($"Id: {i} ");
                    Message message = producer.CreateMessage(msg);

                    string fullBatch = _batch.ToString() + i.ToString();
                    message.ApplicationProperties.Add(AP_BATCH, fullBatch);
                    message.Properties.ReplyTo = "scott";

                    _statsList[0].CreatedMessages++;

                    if (!producer.CircuitBreakerTripped)
                        await producer.SendMessage(message);
                    else
                    {
                        bool keepTrying = true;
                        while (keepTrying)
                        {
                            if (producer.IsCancelled) return;
                            if (producer.CircuitBreakerTripped) Thread.Sleep(2000);
                            else await producer.SendMessage(message);
                        }
                    }

                    if (producer.IsCancelled) return;
                }

                _batch = HelperFunctions.NextBatch(_batch);
                DisplayStats.Refresh();
                Thread.Sleep(IntervalInSecondsBetweenBatches * 1000);
            }
            catch (Exception ex) { }
        }
    }

    private void MessageConfirmationError(object sender, MessageConfirmationEventArgs e)
    {
        _statsList[0].FailureMessages++;
        bool success = e.Status == ConfirmationStatus.Confirmed ? true : false;
        ConfirmationMessage cm = new ConfirmationMessage(e.Message, success, (string)e.Message.ApplicationProperties[AP_BATCH]);
        _statsList[0].ProducedMessages.Add(cm);
        if (_statsList[0].ProducedMessages.Count > statsListSize) { _statsList[0].ProducedMessages.RemoveAt(0); }

        string batch = GetBatch(e);

        HelperFunctions.WriteInColor($"ConfErr:  Msg Batch:  {batch} ", ConsoleColor.Red);
    }

    private void MessageConfirmationSuccess(object sender, MessageConfirmationEventArgs e)
    {
        string batch = GetBatch(e);
        bool success = e.Status == ConfirmationStatus.Confirmed ? true : false;
        ConfirmationMessage cm = new ConfirmationMessage(e.Message, success, (string)e.Message.ApplicationProperties[AP_BATCH]);
        _statsList[0].ProducedMessages.Add(cm);
        if (_statsList[0].ProducedMessages.Count > statsListSize) { _statsList[0].ProducedMessages.RemoveAt(0);}

        _statsList[0].SuccessMessages++;
        //HelperFunctions.WriteInColor($"Success:  MSg Batch:  {batch}", ConsoleColor.Green);
    }


    



    private string GetBatch(MessageConfirmationEventArgs e)
    {
        string batch = "";
        if (e.Message.ApplicationProperties.ContainsKey(AP_BATCH))
        {
            batch = (string)e.Message.ApplicationProperties[AP_BATCH];
        }
        else batch = "Not Specified";

        return batch;
    }
}