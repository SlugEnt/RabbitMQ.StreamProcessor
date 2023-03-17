using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Bogus;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Stream.Client;
using SlugEnt.MQStreamProcessor;

namespace Test_StreamProcessorLibrary
{
    /// <summary>
    /// Custom object we will use as an object to encode into the message
    /// </summary>
    public class TestMsg
    {
        public TestMsg(string name, long id, double amount)
        {
            Name   = name;
            Id     = id;
            Amount = amount;
        }


        public string Name { get; set; }
        public long Id { get; set; }
        public double Amount { get; set; }

        public TestMsg() { }
    }



    [TestFixture]
    public class Test_MessageExtensions
    {
        private IServiceCollection _services;
        private ServiceProvider    _serviceProvider;


        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            _services = new ServiceCollection().AddLogging();
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



        // Test that we can add text to the Application Properties
        [Test]
        public void AddApplicationPropertyText()
        {
            // A. Setup
            string streamA = "sA";
            string appA    = "A1";

            IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
            IMqStreamProducer producerA      = mqStreamEngine.GetProducer(streamA, appA);


            // B. Test
            string msgText       = "Hello World";
            string propertyName  = "someKey";
            string propertyValue = "someValue";

            Message message = producerA.CreateMessage(msgText);
            message.AddApplicationProperty(propertyName, propertyValue);

            // C. Validate
            Assert.AreEqual(propertyValue, message.GetApplicationPropertyAsString(propertyName), "A10:");
        }



        // Tests that we can add an object to the ApplicationProperties
        [Test]
        public void AddApplicationPropertyObject()
        {
            // A. Setup
            string streamA = "sA";
            string appA    = "A1";

            IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
            IMqStreamProducer producerA      = mqStreamEngine.GetProducer(streamA, appA);


            // B. Test
            string  name    = "abcXYZ";
            long    id      = 4045;
            double  amount  = 5406543.43;
            TestMsg testMsg = new TestMsg(name, id, amount);

            string msgText      = "Hello World";
            string propertyName = "someKey";

            Message message = producerA.CreateMessage(msgText);
            message.AddApplicationProperty(propertyName, testMsg);

            // C. Validate
            TestMsg answerMsg = message.GetApplicationProperty<TestMsg>(propertyName);
            Assert.AreEqual(answerMsg.Name, name, "A10:");
            Assert.AreEqual(answerMsg.Id, id, "A10:");
            Assert.AreEqual(answerMsg.Amount, amount, "A10:");
        }


        [Test]
        public void ProducerCreateMessageWithObject()
        {
            // A. Setup
            string streamA = "sA";
            string appA    = "A1";

            IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
            IMqStreamProducer producerA      = mqStreamEngine.GetProducer(streamA, appA);


            // B. Test
            string  name    = "abcXYZ";
            long    id      = 4045;
            double  amount  = 5406543.43;
            TestMsg testMsg = new TestMsg(name, id, amount);

            Message message = producerA.CreateMessage(testMsg);

            TestMsg answerMsg = message.GetObject<TestMsg>();
            Assert.AreEqual(answerMsg.Name, name, "A10:");
            Assert.AreEqual(answerMsg.Id, id, "A10:");
            Assert.AreEqual(answerMsg.Amount, amount, "A10:");
        }



        // Tests that we can add an object to the ApplicationProperties with a >3K application property 
        [Test]
        public void AddApplicationPropertyObjectLarge()
        {
            // A. Setup
            string streamA = "sA";
            string appA    = "A1";

            IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
            IMqStreamProducer producerA      = mqStreamEngine.GetProducer(streamA, appA);


            // B. Test
            var testMsgs = new Faker<TestMsg>()
                           .RuleFor(o => o.Name, f => f.Random.AlphaNumeric(5000))
                           .RuleFor(o => o.Id, f => f.Random.Long(0, 9000000))
                           .RuleFor(o => o.Amount, f => f.Random.Double(0, 9000000));


            TestMsg testMsg = testMsgs.Generate();

            string name   = testMsg.Name;
            long   id     = testMsg.Id;
            double amount = testMsg.Amount;

            string msgText      = "Hello World";
            string propertyName = "someKey";

            Message message = producerA.CreateMessage(msgText);
            message.AddApplicationProperty(propertyName, testMsg);

            // C. Validate
            TestMsg answerMsg = message.GetApplicationProperty<TestMsg>(propertyName);
            Assert.AreEqual(answerMsg.Name, name, "A10:");
            Assert.AreEqual(answerMsg.Id, id, "A10:");
            Assert.AreEqual(answerMsg.Amount, amount, "A10:");
        }


        [Test]
        public void PrintMessageInfo()
        {
            // A. Setup
            string streamA = "sA";
            string appA    = "A1";

            IMQStreamEngine   mqStreamEngine = GetTestingStreamEngine();
            IMqStreamProducer producerA      = mqStreamEngine.GetProducer(streamA, appA);


            // B. Test
            var testMsgs = new Faker<TestMsg>()
                           .RuleFor(o => o.Name, f => f.Random.AlphaNumeric(5000))
                           .RuleFor(o => o.Id, f => f.Random.Long(0, 9000000))
                           .RuleFor(o => o.Amount, f => f.Random.Double(0, 9000000));


            TestMsg testMsg = testMsgs.Generate();

            string msgText      = "Hello World";
            string propertyName = "someKey";
            string subject      = "thisIsSubkject";
            Guid   guid         = Guid.NewGuid();

            Message message = producerA.CreateMessage(msgText);
            message.AddApplicationProperty(propertyName, testMsg);
            message.Properties.Subject       = subject;
            message.Properties.CorrelationId = guid;
            message.AddApplicationProperty("NumberOfInstances", 560564);

            // C. Validate
            string info = message.PrintMessageInfo();

            StringAssert.Contains(subject, info, "A10:");
            StringAssert.Contains(propertyName, info, "A20:");
            StringAssert.Contains(guid.ToString(), info, "A30:");
        }
    }
}