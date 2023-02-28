using RabbitMQ.Stream.Client;
using System.ComponentModel;
using System.Drawing;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using StreamProcessor.Console;
using StreamProcessor.Console.SampleB;
using Microsoft.Extensions.DependencyInjection;
using System.Net;

namespace StreamProcessor.ConsoleScr.SampleB;


public class SampleBApp
{
    // Just the key that is added to all messages 
    private const string AP_BATCH = "Batch";

    private string _streamName = "Sample.B";
    private ISampleB_Producer _producerB;

    // Consumer - Slow
    private ISampleB_Consumer _consumerB_slow;
    private string _consumerB_slow_name = "ConBSlow";

    private ISampleB_Consumer _consumerB_Fast;
    private string _consumerB_fast_name = "ConBFast";
    
    
    private string _batch;
    
    private short statsListSize = 10;
    private List<Stats> _statsList;

    private ILogger<SampleBApp> _logger;
    private IServiceProvider _serviceProvider;


    public SampleBApp(ILogger<SampleBApp> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;


        // TODO - Move this to Appsettings
        StreamSystemConfig config = GetStreamSystemConfig();
        

        // Build a producer
        _producerB = _serviceProvider.GetService<ISampleB_Producer>();
        _producerB.Initialize(_streamName,"producerB",config);
        _producerB.SetProducerMethod(ProduceMessages);
        
        //new SampleB_Producer(_streamName, "Sample.B.Producer", ProduceMessages);
        _producerB.SetStreamLimitsRaw(1000, 100, 900);
        
        _producerB.MessageConfirmationError += MessageConfirmationError;
        _producerB.MessageConfirmationSuccess += MessageConfirmationSuccess;

        _consumerB_slow = _serviceProvider.GetService<ISampleB_Consumer>();
        _consumerB_slow.Initialize(_streamName, _consumerB_slow_name,config);
        _consumerB_slow.SetConsumptionHandler(ConsumeSlow);
        _consumerB_slow.EventCheckPointSaved += OnEventCheckPointSavedSlow;

        _consumerB_Fast = _serviceProvider.GetService<ISampleB_Consumer>();
        _consumerB_Fast.Initialize(_streamName, _consumerB_fast_name,config);
        _consumerB_Fast.SetConsumptionHandler(ConsumeFast);
        _consumerB_Fast.EventCheckPointSaved += OnEventCheckPointSavedFast;


        BuildStatsObjects();
        

        DisplayStats = new DisplayStats(_statsList);
        
        //DisplayStats.Refresh();
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


    public async Task Start()
    {
        try
        {
            await _producerB.Start();
            await _consumerB_slow.Start();
            await _consumerB_Fast.Start();


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
                Thread.Sleep(1000);

            }
        }
        catch (Exception ex) { System.Console.WriteLine("Exception {0}", ex.Message); }

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
                //DisplayStats.Refresh();
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
        _statsList[1].ConsumeCurrentAwaitingCheckpoint = _consumerB_slow.CheckpointOffsetCounter;
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
        _statsList[2].ConsumeCurrentAwaitingCheckpoint = _consumerB_Fast.CheckpointOffsetCounter;
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
                a
            },
        };
        return config;
    }
}
