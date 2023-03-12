using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Castle.DynamicProxy.Generators.Emitters.SimpleAST;
using Microsoft.Extensions.Logging;
using Moq.Protected;
using RabbitMQ.Stream.Client;
using SlugEnt.StreamProcessor;

namespace Test_StreamProcessorLibrary
{
    public class MockMQConsumer : MQStreamBase, IMqStreamConsumer
    {
        // Simulating MQ
        private Queue<Message> _messagesToConsume     = new Queue<Message>();
        private ulong          _msgOffsetID           = 0;
        private ulong          _rabbitMQ_StoredOffset = 0;


        public MockMQConsumer(ILogger<MockMQConsumer> logger) : base(logger, EnumMQStreamType.Consumer) { }

        public ulong LastOffset { get; private set; } = 0;

        public ulong CheckpointLastOffset { get; private set; } = 0;

        public ulong MessagesConsumed => MessageCounter;

        public int CheckpointOffsetCounter { get; protected set; } = 0;

        public int CheckpointOffsetLimit { get; set; } = 20;

        public DateTime CheckpointLastDateTime { get; set; } = DateTime.Now;

        public ulong MessageCounter { get; protected set; }

        public EnumMQStreamType MqStreamType => EnumMQStreamType.Consumer;


        public bool IsConnected { get; protected set; }

        public ulong MaxLength { get; set; }

        public int MaxSegmentSize { get; set; }

        public ulong MaxAge { get; set; }

        public event EventHandler<MqStreamCheckPointEventArgs> EventCheckPointSaved;

        public Task CheckPointSetAsync() { throw new NotImplementedException(); }


        public override Task ConnectAsync()
        {
            IsConnected = true;
            return Task.CompletedTask;
        }


        public Task ConsumeAsync()
        {
            // Do nothing.
            //throw new NotImplementedException();
            return Task.CompletedTask;
        }


        public Task DeleteStream() { return Task.CompletedTask; }


        public void Initialize(string mqStreamName, string applicationName, StreamSystemConfig config)
        {
            _mqStreamName = mqStreamName;
            _appName      = applicationName;
            return;
        }


        public void SetConsumptionHandler(Func<Message, Task<bool>> callHandler)
        {
            // We use an internal Consumption handler!
        }


        public override Task StopAsync()
        {
            IsConnected = false;
            return Task.CompletedTask;
        }


        public Task StreamInfo() { return Task.CompletedTask; }


        //*********************************************************************
        // * Following are new methods to assist with testing.

        /// <summary>
        /// Sets the offset to some starting value
        /// </summary>
        public ulong SetStartingOffset
        {
            get { return _msgOffsetID; }
            set { _msgOffsetID = value; }
        }


        /// <summary>
        /// This is our Simulated RabbitMQ Queue.
        /// </summary>
        public Queue<Message> ConsumerMessageQueue
        {
            get { return _messagesToConsume; }
        }

        /// <summary>
        /// This is the receiving or input side of the Rabbit MQ Queue
        /// </summary>
        public Queue<Message> ProducerMessageQueue { get; set; }
    }
}