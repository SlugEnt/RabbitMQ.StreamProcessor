// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.ComponentModel;
using System.ComponentModel.Design;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Xml.Linq;
using StreamProcessor.Console;
using StreamProcessor.Console.SampleA;
using StreamProcessor.Console.SampleB;
using StreamProcessor.ConsoleScr.SampleB;

Console.WriteLine("MQ Stream Sender");

/*
var loggerFactory = LoggerFactory.Create(builder => {
    builder.AddSimpleConsole();
});
*/

// See if a configuration file exists.  If so read from it, otherwise start a new config
Config config;
string fileName = "Console.config";
if (!File.Exists(fileName))
    config = new Config();
else
{
    using FileStream fileStream = File.OpenRead(fileName);
    config = await JsonSerializer.DeserializeAsync<Config>(fileStream);
}


Console.WriteLine("Select which Program you wish to run.");
Console.WriteLine(" ( 1 )  sample_stream - with Defined Queue Settings.");
Console.WriteLine(" ( 2 )  s2 - with no defined Queue Settings.");
Console.WriteLine(" ( 3 )  s3.1MinQueue - Very small Queue size and age limits.");
Console.WriteLine(" ( A )  Sample A Logic.");
Console.WriteLine(" ( B )  Sample B - Multiple Different Simultaneous Consumers");
Console.WriteLine(" ( 0 )  Test Batches Logic.");
ConsoleKeyInfo key = Console.ReadKey();


string streamName = "";
MqStreamProducer producer = null;
MqStreamConsumer consumer = null;
bool deleteStream = false;


switch (key.Key)
{
    case ConsoleKey.A:
        SampleA sampleA = new SampleA(4);
        sampleA.Start();
        break;

    case ConsoleKey.B:
        SampleB sampleB = new SampleB(4);
        
        sampleB.Start();
        break;

    case ConsoleKey.D0:
        string batch = "A";
        for (int j = 0; j < 30; j++)
        {
            for (int i = 0; i < 26; i++)
            {
                Console.WriteLine($"{batch}");
                batch = HelperFunctions.NextBatch(batch);

            }
        }

        break;

    // Simple stream with defined sizes set by us.  Once set, these can never be changed for the lifetime of the Queue, not the App!
    case ConsoleKey.D1:
        StreamS2NoLimits streamA = new StreamS2NoLimits();
        await streamA.Start();
        bool keepProcessingA = true;
        while (keepProcessingA)
        {
            if (Console.KeyAvailable)
            {
                ConsoleKeyInfo d1KeyInfo = Console.ReadKey();
                if (d1KeyInfo.Key == ConsoleKey.X)
                {
                    keepProcessingA= false;
                    await streamA.Stop();
                }
            }

            streamA.CheckStatus();
            Thread.Sleep(1000);

        }

        Console.WriteLine($"Stream {streamA.StreamName} has completed all processing.");
        break;

    // Simple stream with defined sizes set by us.  Once set, these can never be changed for the lifetime of the Queue, not the App!
    case ConsoleKey.D2:

        break;


    // Simple stream, but no defined queue parameters - we let RabbitMQ and it's policies define the parameters.
    // This enables dynamic real time changes.
    case ConsoleKey.D3:
        streamName = "s2.NoLimits";
        // Produce
        producer = new MqStreamProducer(streamName,"d3");
        producer.SetNoStreamLimits();
        await producer.ConnectAsync();
        await producer.StartAsync();

        // Lets Start a Consumer
        consumer = new MqStreamConsumer(streamName,"d3");
        await consumer.ConnectAsync();
        consumer.SetConsumptionHandler(ConsumeMessageHandler);
        await consumer.ConsumeAsync();

        deleteStream = true;        
        break;

    case ConsoleKey.D4:
        streamName = "s3A.1MinQueue";
        // Produce
        producer = new MqStreamProducer(streamName,"d4");
        producer.SetStreamLimitsRaw(10000,500,60);
        await producer.ConnectAsync();
        await producer.StartAsync();

        // Lets Start a Consumer
        consumer = new MqStreamConsumer(streamName,"d4");
        await consumer.ConnectAsync();
        consumer.SetConsumptionHandler(ConsumeMessageHandler);
        await consumer.ConsumeAsync();

        deleteStream = true;
        break;
}

Thread.Sleep(2000);
Console.WriteLine("Press any key to exit the application.  Press D to delete the stream");
ConsoleKeyInfo key2 = Console.ReadKey();

if ((key2.Key == ConsoleKey.D) && (deleteStream)) consumer.DeleteStream();


/*
var producerLogger = loggerFactory.CreateLogger<Producer>();
var consumerLogger = loggerFactory.CreateLogger<Consumer>();
var streamLogger = loggerFactory.CreateLogger<StreamSystem>();
*/


async Task<bool> ConsumeMessageHandler(Message message)
{
    //_counter++;
    int _counter = 0;
    string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
    Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
    await Task.CompletedTask;
    return true;
}


