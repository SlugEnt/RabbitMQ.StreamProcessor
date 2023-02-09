using RabbitMQ.Stream.Client;
using SlugEnt.StreamProcessor;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor.Console
{
    internal class StreamS2NoLimits
    {
        private readonly string _streamName;
        readonly StreamProducer _producer = null;
        readonly StreamConsumer _consumer = null;
        private List<string> _messages = new List<string>();
        private int _counter = 0;
        private BackgroundWorker _bgwMsgSender;
        private BackgroundWorker _bgwMsgReceiver;
        private long _receiveCounter;


        internal StreamS2NoLimits()
        {
            _streamName = "sample_stream";
            _producer = new StreamProducer(_streamName);
            _consumer = new StreamConsumer(_streamName,"S2Test");

        }

        internal string StreamName
        {
            get { return _streamName;}
        }


        /// <summary>
        /// The Message consumer should call this each time a message is received.
        /// </summary>
        internal long ReceiveCounter
        {
            get { return _receiveCounter; }
            private set
            {
                _receiveCounter++;
                ReceiveSinceLastCheck++;
                _consumer.OffsetCounter++;
            }
        }

        internal long SendCounter { get; private set; } = 0;

        /// <summary>
        /// Messages received since last check interval
        /// </summary>
        internal int ReceiveSinceLastCheck { get; private set; } = 0;


        /// <summary>
        /// Messages Sent since last check interval
        /// </summary>
        internal int SendSinceLastCheck { get; private set; } = 0;



        /// <summary>
        /// How many messages are consumed / received before we send an update to the console.
        /// </summary>
        internal short ReceiveStatusUpdateInterval { get; set; } = 10;

        /// <summary>
        /// How many messages are sent before we send an update to the console
        /// </summary>
        internal short SendStatusUpdateInterval { get; set; } = 10;



        internal async Task Start()
        {
            await _producer.ConnectAsync();
            await _consumer.ConnectAsync();


            // Setup Background Producer
            await _producer.PublishAsync();
            _bgwMsgSender = new BackgroundWorker();
            _bgwMsgSender.DoWork += producer_DoWork;
            _bgwMsgSender.ProgressChanged += ProducerProgressChanged;
            _bgwMsgSender.RunWorkerCompleted += ProducerCompleted;
            _bgwMsgSender.WorkerReportsProgress = false;
            _bgwMsgSender.WorkerSupportsCancellation = true;
            _bgwMsgSender.RunWorkerAsync();


            // Consumer is a little different.  We go ahead and set the callback and tell it to start consuming.
            // The background worker merely checks to see if we should stop and prints a message out periodically
            _consumer.SetConsumptionHandler(ConsumeMessageHandler);
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
            System.Console.WriteLine($"  Produced:    {SendCounter}");
            System.Console.WriteLine($"  Consumed:    {ReceiveCounter}");

        }


        internal void CheckStatus()
        {
            if (SendSinceLastCheck > SendStatusUpdateInterval)
            {
                System.Console.WriteLine(
                    $"Produced:  {DateTime.Now.ToString("F")} -->  {SendSinceLastCheck} messages.  Total: {SendCounter}");
                SendSinceLastCheck = 0;
            }

            if (ReceiveSinceLastCheck > ReceiveStatusUpdateInterval)
            {
                System.Console.WriteLine($"Consumed:  {DateTime.Now.ToString("F")} -->  {ReceiveSinceLastCheck} messages.  Total: {ReceiveCounter}");
                ReceiveSinceLastCheck = 0;
            }
        }


#region "Producer_Background_Worker"
        private void producer_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            SendMessages(worker);

            e.Cancel = true;
        }

        private void SendMessages(BackgroundWorker worker)
        {
            while (!_bgwMsgSender.CancellationPending)
            {
                // Publish the messages
                for (var i = 0; i < 10; i++)
                {
                    DateTime x = DateTime.Now;
                    string timeStamp = x.ToString("F");
                    string msg = String.Format("Time: {0}   -->  Batch Msg # {1}", timeStamp, i);
                    _producer.SendMessage(msg);
                    SendCounter++;
                    SendSinceLastCheck++;
                }

                Thread.Sleep(10000);
            }

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


        public async Task ConsumeMessageHandler(string streamName, RawConsumer consumer, MessageContext msgContext, Message message)
        {
            ReceiveCounter++;

            string x = Encoding.Default.GetString(message.Data.Contents.ToArray());
            //_messages.Add(x);
            if (_consumer.OffsetCounter >= _consumer.OffsetCheckPointLimit)
            {
                _consumer.CheckpointOffset(consumer, msgContext);
            }

            await Task.CompletedTask;


        }


    }
}
