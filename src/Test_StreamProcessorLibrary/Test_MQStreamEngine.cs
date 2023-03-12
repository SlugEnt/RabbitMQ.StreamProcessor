using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using SlugEnt.MQStreamProcessor;

namespace Test_StreamProcessorLibrary;

[TestFixture]
public class Test_MQStreamEngine
{
    private IServiceCollection _services;
    private ServiceProvider    _serviceProvider;


    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _services = new ServiceCollection().AddLogging();
        _services.AddTransient<IMqStreamConsumer, MockMQConsumer>();
        _services.AddTransient<IMQStreamEngine, MQStreamEngine>();
        _services.AddTransient<IMqStreamProducer, MockMQProducer>();
        _serviceProvider = _services.BuildServiceProvider();
    }



    /// <summary>
    /// Builds the MQStream Engine and returns it
    /// </summary>
    /// <returns></returns>
    private IMQStreamEngine GetTestingStreamEngine()
    {
        // B. Test
        IMQStreamEngine    mqStreamEngine = _serviceProvider.GetService<IMQStreamEngine>();
        StreamSystemConfig config         = new StreamSystemConfig();
        mqStreamEngine.StreamSystemConfig = config;
        return mqStreamEngine;
    }



    // When getting a consumer, the consumer is added to the dictionary with a key of its fullname
    [Test]
    public void GetConsumer_AddToConsumerDictionary()
    {
        // A. Setup
        string streamName = "sA";
        string appName    = "A1";

        // B. Test
        IMQStreamEngine    mqStreamEngine = _serviceProvider.GetService<IMQStreamEngine>();
        StreamSystemConfig config         = new StreamSystemConfig();
        mqStreamEngine.StreamSystemConfig = config;
        IMqStreamConsumer consumer = mqStreamEngine.GetConsumer(streamName, appName, dummyConsumer);

        // C. Validate
        Assert.IsNotNull(consumer, "C200:");
        Assert.AreEqual(1, mqStreamEngine.StreamConsumersDictionary.Count, "C210:");
        Assert.IsTrue(mqStreamEngine.StreamConsumersDictionary.ContainsKey(consumer.FullName), "C220:");
    }


    /// <summary>
    /// When getting a consumer, if StreamConfig not set an error is thrown
    /// </summary>
    [Test]
    public void StreamConfigNotSet_ThrowsExceptionIfGetConsumerCalled()
    {
        // A. Setup
        string streamName = "sA";
        string appName    = "A1";

        // B. Test
        IMQStreamEngine mqStreamEngine = _serviceProvider.GetService<IMQStreamEngine>();

        // C. Validate
        Assert.Throws<ApplicationException>(() => mqStreamEngine.GetConsumer(streamName, appName, dummyConsumer));
    }


    /// <summary>
    /// Builds 3 consumers and verifies Start All, results in all 3 being started.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task StartAllConsumers_Succeeds()
    {
        // A. Setup
        string streamA = "sA";
        string appA    = "A1";
        string streamB = "sB";
        string appB    = "B1";
        string streamC = "sC";
        string appC    = "C1";


        // B. Test
        IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
        IMqStreamConsumer consumerA      = mqStreamEngine.GetConsumer(streamA, appA, dummyConsumer);
        IMqStreamConsumer consumerB      = mqStreamEngine.GetConsumer(streamB, appB, dummyConsumer);
        IMqStreamConsumer consumerC      = mqStreamEngine.GetConsumer(streamC, appC, dummyConsumer);


        // C. PreValidation
        Assert.IsFalse(consumerA.IsConnected, "C200");
        Assert.IsFalse(consumerB.IsConnected, "C210");
        Assert.IsFalse(consumerC.IsConnected, "C220");

        // D. Start them
        await mqStreamEngine.StartAllConsumersAsync();

        // E. Re-validate
        Assert.IsTrue(consumerA.IsConnected, "E400");
        Assert.IsTrue(consumerB.IsConnected, "E410");
        Assert.IsTrue(consumerC.IsConnected, "E420");
    }


    [Test]
    public async Task StopAllConsumers_Success()
    {
        // A. Setup
        string streamA = "sA";
        string appA    = "A1";
        string streamB = "sB";
        string appB    = "B1";
        string streamC = "sC";
        string appC    = "C1";


        IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
        IMqStreamConsumer consumerA      = mqStreamEngine.GetConsumer(streamA, appA, dummyConsumer);
        IMqStreamConsumer consumerB      = mqStreamEngine.GetConsumer(streamB, appB, dummyConsumer);
        IMqStreamConsumer consumerC      = mqStreamEngine.GetConsumer(streamC, appC, dummyConsumer);


        // B. Pre-Validation
        Assert.IsFalse(consumerA.IsConnected, "B200");
        Assert.IsFalse(consumerB.IsConnected, "B210");
        Assert.IsFalse(consumerC.IsConnected, "B220");

        // C. Start them
        await mqStreamEngine.StartAllConsumersAsync();

        // D. Re-validate
        Assert.IsTrue(consumerA.IsConnected, "D400");
        Assert.IsTrue(consumerB.IsConnected, "D410");
        Assert.IsTrue(consumerC.IsConnected, "D420");

        // E. Stop them
        await mqStreamEngine.StopAllConsumersAsync();

        // F. Re-validate
        Assert.IsFalse(consumerA.IsConnected, "D500");
        Assert.IsFalse(consumerB.IsConnected, "D510");
        Assert.IsFalse(consumerC.IsConnected, "D520");
    }



    /// <summary>
    /// Builds 3 producers and verifies Start All, results in all 3 being started.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task StartAllProducers_Succeeds()
    {
        // A. Setup
        string streamA = "sA";
        string appA    = "A1";
        string streamB = "sB";
        string appB    = "B1";
        string streamC = "sC";
        string appC    = "C1";


        // B. Test
        IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
        IMqStreamProducer producerA      = mqStreamEngine.GetProducer(streamA, appA);
        IMqStreamProducer producerB      = mqStreamEngine.GetProducer(streamB, appB);
        IMqStreamProducer producerC      = mqStreamEngine.GetProducer(streamC, appC);


        // C. PreValidation
        Assert.IsFalse(producerA.IsConnected, "C200");
        Assert.IsFalse(producerB.IsConnected, "C210");
        Assert.IsFalse(producerC.IsConnected, "C220");

        // D. Start them
        await mqStreamEngine.StartAllProducersAsync();

        // E. Re-validate
        Assert.IsTrue(producerA.IsConnected, "E400");
        Assert.IsTrue(producerB.IsConnected, "E410");
        Assert.IsTrue(producerC.IsConnected, "E420");
    }



    /// <summary>
    /// Just a dummy consumer.  Is never used.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<bool> dummyConsumer(Message message) { return true; }
}