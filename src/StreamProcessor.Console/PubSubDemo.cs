using RabbitMQ.Stream.Client;
using SlugEnt.StreamProcessor;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices.JavaScript;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor.Console
{
    internal class PubSubDemo
    {
        private readonly string _streamName;
        readonly MqStreamProducer _producer = null;
        readonly MqStreamConsumer _consumer = null;
        private List<string> _messages = new List<string>();
        private int _counter = 0;
        private BackgroundWorker _bgwMsgSender;
        private BackgroundWorker _bgwMsgReceiver;
        private Func<PubSubDemo,MqStreamProducer, BackgroundWorker, Task<bool>> _producerHandler;
        private Func<Message, Task<bool>> _consumerHandler;


        internal PubSubDemo(string streamName, 
            string appName, 
            Func<PubSubDemo,MqStreamProducer, BackgroundWorker,Task<bool>> producerMethod,
            Func<Message,Task<bool>> messageHandler)
        {
            _streamName = streamName;
            _producer = new MqStreamProducer(_streamName,appName);
            _consumer = new MqStreamConsumer(_streamName, appName);

            _producerHandler = producerMethod;
            _consumerHandler = messageHandler;
        }


        internal string StreamName
        {
            get { return _streamName; }
        }




        /// <summary>
        /// Messages received since last check interval
        /// </summary>
        public int ReceiveSinceLastCheck { get;  set; } = 0;


        /// <summary>
        /// Messages Sent since last check interval
        /// </summary>
        public int SendSinceLastCheck { get; set; } = 0;



        /// <summary>
        /// How many messages are consumed / received before we send an update to the console.
        /// </summary>
        internal short ReceiveStatusUpdateInterval { get; set; } = 10;

        /// <summary>
        /// How many messages are sent before we send an update to the console
        /// </summary>
        internal short SendStatusUpdateInterval { get; set; } = 10;


        public MqStreamProducer Producer
        {
            get { return _producer; }
        }

        public MqStreamConsumer Consumer
        {
            get { return _consumer; }
        }


        internal async Task Start()
        {
            // Set the limits for the stream in case it does not exist.

            await _producer.ConnectAsync();
            await _consumer.ConnectAsync();


            // Setup Background Producer
            await _producer.StartAsync();
            _bgwMsgSender = new BackgroundWorker();
            _bgwMsgSender.DoWork += producer_DoWork;
            _bgwMsgSender.ProgressChanged += ProducerProgressChanged;
            _bgwMsgSender.RunWorkerCompleted += ProducerCompleted;
            _bgwMsgSender.WorkerReportsProgress = false;
            _bgwMsgSender.WorkerSupportsCancellation = true;
            _bgwMsgSender.RunWorkerAsync();


            // Consumer is a little different.  We go ahead and set the callback and tell it to start consuming.
            // The background worker merely checks to see if we should stop and prints a message out periodically
            //_consumer.SetConsumptionHandler(ConsumeMessageHandler);
            _consumer.SetConsumptionHandler(_consumerHandler);
            await _consumer.ConsumeAsync();


            _bgwMsgReceiver = new BackgroundWorker();
            _bgwMsgReceiver.DoWork += Consumer_DoWork;
            _bgwMsgReceiver.RunWorkerCompleted += ConsumerCompleted;
            _bgwMsgReceiver.WorkerSupportsCancellation = true;


            // Start Consuming messages
            System.Console.WriteLine("Starting to Consume Messages");
            _bgwMsgReceiver.RunWorkerAsync();
        }


        internal async Task Stop()
        {
            _bgwMsgSender.CancelAsync();
            _bgwMsgReceiver.CancelAsync();


            // Should save offset

            // Print Final Totals
            System.Console.WriteLine("Messages:");
            System.Console.WriteLine($"  Produced:    {_producer.MessageCounter}");
            //System.Console.WriteLine($"  Consumed:    {ReceiveCounter}");

        }



        internal async Task CheckStatus()
        {
            if (SendSinceLastCheck > SendStatusUpdateInterval)
            {
                System.Console.WriteLine(
                    $"Produced:  {DateTime.Now.ToString("F")} -->  {SendSinceLastCheck} messages.  Total: {_producer.MessageCounter}");
                SendSinceLastCheck = 0;
            }

            if (_consumer.OffsetCounter > _consumer.OffsetCheckPointLimit)
            {
                System.Console.WriteLine($"Consumed:  {DateTime.Now.ToString("F")} -->  {_consumer.OffsetCounter} messages.  Total: {_consumer.MessageCounter}");
                await _consumer.CheckPointSet();
                ReceiveSinceLastCheck = 0;
            }
        }





#region "Producer_Background_Worker"
        private void producer_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            ProduceMessages(worker);

            e.Cancel = true;
        }


        /// <summary>
        /// Calls the method to produce messages.  That method does not return until done.
        /// </summary>
        /// <param name="worker"></param>
        private void ProduceMessages(BackgroundWorker worker)
        {
            _producerHandler(this, _producer, _bgwMsgSender);
        }


        private void ProducerProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            System.Console.WriteLine("Just Produced some more Messages");
        }

        private void ProducerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled) System.Console.WriteLine("Producer was cancelled");
            else if (e.Error != null) System.Console.WriteLine("Producer had an error - {0}", e.Error.Message);
            else System.Console.WriteLine("Producer finished sending messages successfully");
        }
        #endregion



        #region "Consumer_Background_Worker"

        private void Consumer_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            ConsumeMessages(worker);
            e.Cancel = true;
        }

        private void ConsumeMessages(BackgroundWorker worker)
        {
            while (!_bgwMsgReceiver.CancellationPending)
            {
                Thread.Sleep(2000);
            }
        }



        private void ConsumerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled) System.Console.WriteLine("Consumer was cancelled");
            else if (e.Error != null) System.Console.WriteLine("Consumer had an error - {0}", e.Error.Message);
            else System.Console.WriteLine("Consumer has stopped successfully");
        }
        #endregion


        public async Task<bool> ConsumeMessageHandler(Message message)
        {
            
            //string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
            //_messages.Add(x);

            await Task.CompletedTask;

            return true;
        }


    }
}
