using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using StreamProcessor.Console.SampleB;
using StreamProcessor.Console;
using StreamProcessor.ConsoleScr.SampleB;
using System.Net;
using Microsoft.Extensions.DependencyInjection;

namespace StreamProcessor.ConsoleScr.SampleC;

public class SampleCApp
{
    // Just the key that is added to all messages 
    private const string AP_BATCH = "Batch";

    private string _streamName = "Sample.C";

    private IMQStreamEngine _mqStreamEngine;
    private IMqStreamProducer _producer;
    private IMqStreamConsumer _consumerSlow;
    private IMqStreamConsumer _consumerFast;

    private bool _stopping = false;

    private string _batch;

    private short statsListSize = 10;
    private List<Stats> _statsList;

    private ILogger<SampleBApp> _logger;
    private IServiceProvider _serviceProvider;



    public SampleCApp(ILogger<SampleBApp> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;


        // TODO - Move this to Appsettings
        StreamSystemConfig config = GetStreamSystemConfig();

        _mqStreamEngine =_serviceProvider.GetService<IMQStreamEngine>();
        _mqStreamEngine.StreamSystemConfig = config;

        // Get Producer / Consumers
        _consumerSlow = _mqStreamEngine.GetConsumer(_streamName,"ConCSlow",ConsumeSlow);
        _consumerFast = _mqStreamEngine.GetConsumer(_streamName, "ConCFast", ConsumeFast);

        _producer = _mqStreamEngine.GetProducer(_streamName, "Produce1");
    }


    /// <summary>
    /// Startup the producers and consumers.
    /// </summary>
    /// <returns></returns>
    public async Task Start()
    {
        try
        {
            await _mqStreamEngine.StartAllStreamsAsync();

            // Note - we do not await this!  
            Task produceMessages =  ProduceMessages();

            bool keepProcessingB = true;
            while (keepProcessingB)
            {
                if (System.Console.KeyAvailable)
                {
                    ConsoleKeyInfo d1KeyInfo = System.Console.ReadKey();
                    if (d1KeyInfo.Key == ConsoleKey.X)
                    {
                        keepProcessingB = false;
                        _stopping = true;
                        await produceMessages;
                        return;
                    }
                }

                System.Console.WriteLine($"Queued Messages: {_producer.Stat_RetryQueuedMessageCount}");
                System.Console.WriteLine($"Message Counter: {_producer.MessageCounter}");
                System.Console.WriteLine($"Circuit Breaker Status: {_producer.CircuitBreakerTripped}");

                UpdateStats();
                DisplayStats.Refresh();
                Thread.Sleep(1000);

            }
        }
        catch (Exception ex) { System.Console.WriteLine("Exception {0}", ex.Message); }

        await _mqStreamEngine.StopAllAsync();

        System.Console.WriteLine($"Sample C has completed all processing.");
    }



    private void BuildStatsObjects()
    {
        _statsList = new List<Stats>();
        Stats producerStats = new Stats("Producer");
        Stats consumerStatsA = new Stats("Cons Slow");
        Stats consumerStatsB = new Stats("Cons Fast");
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
    public int IntervalInSecondsBetweenBatches { get; set; } = 3;



    private void UpdateStats()
    {
        _statsList[0].SuccessMessages = _producer.Stat_MessagesSuccessfullyConfirmed;
        _statsList[0].FailureMessages = _producer.Stat_MessagesErrored;
        _statsList[0].CircuitBreakerTripped = _producer.CircuitBreakerTripped;

    }


    /// <summary>
    /// Produces Messages
    /// </summary>
    /// <param name="producer"></param>
    /// <returns></returns>
    protected async Task ProduceMessages()
    {
        // Initiate the Batch Number
        _batch = "A";
        while (!_stopping)
        {
            try
            {
                // Publish the messages
                for (short i = 0; i < MessagesPerBatch; i++)
                {
                    string fullBatchId = _batch + i;
                    string msg = String.Format($"Id: {i} ");
                    Message message = _producer.CreateMessage(msg);

                    string fullBatch = _batch.ToString() + i.ToString();
                    message.ApplicationProperties.Add(AP_BATCH, fullBatch);
                    message.Properties.ReplyTo = "scott";

                    _statsList[0].CreatedMessages++;

                    if (!_producer.CircuitBreakerTripped)
                        await _producer.SendMessageAsync(message);
                    else
                    {
                        bool keepTrying = true;
                        while (keepTrying)
                        {
                            if (_stopping) return;
                            if (_producer.CircuitBreakerTripped) Thread.Sleep(2000);
                            else await _producer.SendMessageAsync(message);
                        }
                    }

                    if (_stopping) break;
                }

                _batch = HelperFunctions.NextBatch(_batch);
                //DisplayStats.Refresh();
                Thread.Sleep(IntervalInSecondsBetweenBatches * 1000);
            }
            catch (Exception ex) { }
        }

        await _producer.StopAsync();
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
        if (_statsList[0].ProducedMessages.Count > statsListSize) { _statsList[0].ProducedMessages.RemoveAt(0); }

        _statsList[0].SuccessMessages++;
        //HelperFunctions.WriteInColor($"Success:  MSg Batch:  {batch}", ConsoleColor.Green);
    }


    private void OnEventCheckPointSavedSlow(object sender, MqStreamCheckPointEventArgs e)
    {
        _statsList[1].ConsumeLastCheckpoint = e.CommittedOffset;
    }


    private void OnEventCheckPointSavedFast(object sender, MqStreamCheckPointEventArgs e)
    {
        _statsList[2].ConsumeLastCheckpoint = e.CommittedOffset;
    }


    /// <summary>
    /// The Consumer Slow Method
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<bool> ConsumeSlow(Message message)
    {
        _statsList[1].ConsumedMessages++;
        _statsList[1].ConsumeLastBatchReceived = (string)message.ApplicationProperties[AP_BATCH];
        //        _statsList[1].ConsumeLastCheckpoint = _consumerB_slow.CheckpointLastOffset;
        _statsList[1].ConsumeCurrentAwaitingCheckpoint = _consumerSlow.CheckpointOffsetCounter;
        // Simulate slow
        Thread.Sleep(1500);
        return true;
    }


    /// <summary>
    /// The Consumer Fast Method
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<bool> ConsumeFast(Message message)
    {
        _statsList[2].ConsumedMessages++;
        _statsList[2].ConsumeLastBatchReceived = (string)message.ApplicationProperties[AP_BATCH];
        _statsList[2].ConsumeCurrentAwaitingCheckpoint = _consumerFast.CheckpointOffsetCounter;
        return true;
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


    /// <summary>
    /// Defines the configuration for connecting to RabbitMQ Servers
    /// </summary>
    /// <returns></returns>
    private StreamSystemConfig GetStreamSystemConfig()
    {
        IPEndPoint a = SlugEnt.StreamProcessor.Helpers.GetIPEndPointFromHostName("rabbitmqa.slug.local", 5552);
        IPEndPoint b = SlugEnt.StreamProcessor.Helpers.GetIPEndPointFromHostName("rabbitmqb.slug.local", 5552);
        IPEndPoint c = SlugEnt.StreamProcessor.Helpers.GetIPEndPointFromHostName("rabbitmqc.slug.local", 5552);

        StreamSystemConfig config = new StreamSystemConfig
        {
            UserName = "testUser",
            Password = "TESTUSER",
            VirtualHost = "Test",
            Endpoints = new List<EndPoint> {
                a,b,c
            },
        };
        return config;
    }
}