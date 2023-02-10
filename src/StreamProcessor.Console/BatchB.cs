using RabbitMQ.Stream.Client;
using SlugEnt.StreamProcessor;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client.AMQP;

namespace StreamProcessor.Console
{
    internal class BatchB
    {
        private PubSubDemo streamB;
        private string _batch;

        public BatchB()
        {
            streamB = new PubSubDemo("B", "appB", B_Producer, B_Consumer);
            streamB.Producer.SetStreamLimits(100, 20, 24);
            streamB.Consumer.CheckPointPerformedEvent += CheckPointEventHandler;
            _batch = "A";
        }

        public async Task Execute()
        {
            await streamB.Start();
        }



        public async Task Stop()
        {
            await streamB.Stop();
        }



        private void CheckPointEventHandler(string eventDescription, ulong checkPointOffset)
        {
            System.Console.WriteLine($"Event Received:  {eventDescription} # {checkPointOffset}");
        }


        async Task<bool> B_Producer(PubSubDemo demo, MqStreamProducer producer, BackgroundWorker bgwMsgSender)
        {
            while (!bgwMsgSender.CancellationPending)
            {
                // Publish the messages
                for (var i = 0; i < 30; i++)
                {
                    DateTime x = DateTime.Now;
                    string timeStamp = x.ToString("F");
                    //string msg = String.Format("Time: {0}   -->  Batch Msg # {1}", timeStamp, i);
                    string msg = String.Format($"ID: {i} hello");

                    Message message = producer.CreateMessage(msg);
                    message.ApplicationProperties = new ApplicationProperties();
                    message.ApplicationProperties.Add("Batch",_batch);
                    producer.SendMessage(message);
                    demo.SendSinceLastCheck++;
                }

                _batch = NextBatch(_batch);

                Thread.Sleep(3000);
            }
            return true;
        }



        async Task<bool> B_Consumer(Message message)
        {
            string x = Encoding.Default.GetString(message.Data.Contents.ToArray());

            if (message.ApplicationProperties != null) 
                System.Console.WriteLine($"Recv: {message.ApplicationProperties["Batch"]} msg: {x}");
            else
            {
                System.Console.WriteLine($"Received message without AppProperties: {x}");
            }
            //_messages.Add(x);

            await Task.CompletedTask;

            return true;
        }

        private string NextBatch(string currentBatch)
        {
            byte z = (byte)'Z';
            byte[] batchIDs = Encoding.ASCII.GetBytes(currentBatch);

            int lastIndex = batchIDs.Length - 1;
            int currentIndex = lastIndex;
            bool continueLooping = true;

            while (continueLooping)
            {
                if (batchIDs[currentIndex] == z)
                {
                    if (currentIndex == 0)
                    {
                        // Append a new column
                        batchIDs[currentIndex] = (byte)'A';
                        string newBatch = Encoding.ASCII.GetString(batchIDs) + "A";
                        return newBatch;
                    }

                    // Change this index to A and move to the prior index.
                    batchIDs[currentIndex] = (byte)'A';
                    currentIndex--;
                }

                // Just increment this index to next letter
                else
                {
                    batchIDs[currentIndex]++;
                    return Encoding.ASCII.GetString(batchIDs);
                }
            }

            // Should never get here.
            return currentBatch;
        }
    }
}


