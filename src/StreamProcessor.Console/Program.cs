// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Diagnostics.Metrics;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;


Console.WriteLine("MQ Stream Sender");

/*
var loggerFactory = LoggerFactory.Create(builder => {
    builder.AddSimpleConsole();
});
*/



StreamProducer streamSend = new StreamProducer("sample_stream");
await streamSend.ConnectAsync();
await streamSend.PublishAsync();


// Lets Start a Consumer
StreamConsumer consumer = new StreamConsumer("sample_stream");
await consumer.ConnectAsync();
consumer.SetConsumptionHandler(ConsumeMessageHandler);


await consumer.ConsumeAsync();

/*
var producerLogger = loggerFactory.CreateLogger<Producer>();
var consumerLogger = loggerFactory.CreateLogger<Consumer>();
var streamLogger = loggerFactory.CreateLogger<StreamSystem>();
*/

Thread.Sleep(4000);


 async Task ConsumeMessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
{
    //_counter++;
    int _counter = 0;
    string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
    Console.WriteLine("Consumed Msg:  # {0} --> {1}", _counter, x);
    await Task.CompletedTask;

}
