// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Xml.Linq;


Console.WriteLine("MQ Stream Sender");

/*
var loggerFactory = LoggerFactory.Create(builder => {
    builder.AddSimpleConsole();
});
*/

Console.WriteLine("Select which Program you wish to run.");
Console.WriteLine(" ( 1 )  sample_stream - with Defined Queue Settings.");
Console.WriteLine(" ( 2 )  s2 - with no defined Queue Settings.");
ConsoleKeyInfo key = Console.ReadKey();


string streamName = "";
StreamProducer producer = null;
StreamConsumer consumer = null;
bool deleteStream = false;


switch (key.Key)
{
    // Simple stream with defined sizes set by us.  Once set, these can never be changed for the lifetime of the Queue, not the App!
    case ConsoleKey.D1:
        streamName = "sample_stream";


        // Produce
        producer = new StreamProducer(streamName);

        producer.SetStreamLimitsSmall();
        await producer.ConnectAsync();
        await producer.PublishAsync();

        // Lets Start a Consumer
        consumer = new StreamConsumer(streamName);
        consumer.SetStreamLimitsSmall();
        await consumer.ConnectAsync();
        consumer.SetConsumptionHandler(ConsumeMessageHandler);
        await consumer.ConsumeAsync();
        break;
    
    // Simple stream, but no defined queue parameters - we let RabbitMQ and it's policies define the parameters.
    // This enables dynamic real time changes.
    case ConsoleKey.D2:
        streamName = "s2.NoLimits";


        // Produce
        producer = new StreamProducer(streamName);

        //producer.SetStreamLimits(1,1,61);
        producer.SetNoStreamLimits();
        await producer.ConnectAsync();
        await producer.PublishAsync();

        // Lets Start a Consumer
        consumer = new StreamConsumer(streamName);
        consumer.SetNoStreamLimits();
        //consumer.SetStreamLimits(1, 1, 61);
        await consumer.ConnectAsync();
        consumer.SetConsumptionHandler(ConsumeMessageHandler);
        await consumer.ConsumeAsync();

        deleteStream = true;        
        break;
}

Console.WriteLine("Press any key to exit the application");
Console.ReadKey();

if (deleteStream) consumer.DeleteStream();


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
