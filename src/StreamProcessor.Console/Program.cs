// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Xml.Linq;
using StreamProcessor.Console;


Console.WriteLine("MQ Stream Sender");

/*
var loggerFactory = LoggerFactory.Create(builder => {
    builder.AddSimpleConsole();
});
*/

Console.WriteLine("Select which Program you wish to run.");
Console.WriteLine(" ( 1 )  sample_stream - with Defined Queue Settings.");
Console.WriteLine(" ( 2 )  s2 - with no defined Queue Settings.");
Console.WriteLine(" ( 3 )  s3.1MinQueue - Very small Queue size and age limits.");
ConsoleKeyInfo key = Console.ReadKey();


string streamName = "";
StreamProducer producer = null;
StreamConsumer consumer = null;
bool deleteStream = false;


switch (key.Key)
{
    // Simple stream with defined sizes set by us.  Once set, these can never be changed for the lifetime of the Queue, not the App!
    case ConsoleKey.D1:
        StreamS2NoLimits stream = new StreamS2NoLimits();
        await stream.ExecuteAsync();
/*        streamName = "sample_stream";


        // Produce
        producer = new StreamProducer(streamName);
        //producer.SetStreamLimitsSmall();
        await producer.ConnectAsync();
        await producer.PublishAsync();
        await producer.StreamInfo();

        // Lets Start a Consumer
        consumer = new StreamConsumer(streamName);
        await consumer.ConnectAsync();
        consumer.SetConsumptionHandler(ConsumeMessageHandler);
        await consumer.ConsumeAsync();
*/
        break;
    
    // Simple stream, but no defined queue parameters - we let RabbitMQ and it's policies define the parameters.
    // This enables dynamic real time changes.
    case ConsoleKey.D2:
        streamName = "s2.NoLimits";
        // Produce
        producer = new StreamProducer(streamName);
        producer.SetNoStreamLimits();
        await producer.ConnectAsync();
        await producer.PublishAsync();

        // Lets Start a Consumer
        consumer = new StreamConsumer(streamName);
        await consumer.ConnectAsync();
        consumer.SetConsumptionHandler(ConsumeMessageHandler);
        await consumer.ConsumeAsync();

        deleteStream = true;        
        break;

    case ConsoleKey.D3:
        streamName = "s3A.1MinQueue";
        // Produce
        producer = new StreamProducer(streamName);
        producer.SetStreamLimitsRaw(10000,500,60);
        await producer.ConnectAsync();
        await producer.PublishAsync();

        // Lets Start a Consumer
        consumer = new StreamConsumer(streamName);
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


async Task ConsumeMessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
{
    //_counter++;
    int _counter = 0;
    string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
    Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
    await Task.CompletedTask;

}
