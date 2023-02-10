﻿
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Text;

namespace SlugEnt.StreamProcessor
{
    public class MqStreamConsumer : MQStreamBase
    {
        //private Func<string, RawConsumer, MessageContext, Message, Task> _callHandler;
        private Func<Message, Task<bool>> _callHandler;
        private readonly string _consumerApplicationName;
        private IConsumer _rawConsumer;


        /// <summary>
        /// Whenever the Queue is checkpointed this event is raised.
        /// </summary>
        public event Action<string, ulong> CheckPointPerformedEvent;


        /// <summary>
        /// Creates a Stream Consumer object
        /// </summary>
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// </param>
        public MqStreamConsumer(string mqStreamName, string applicationName) : base(mqStreamName, applicationName, EnumMQStreamType.Consumer)
        {

        }


        /// <summary>
        /// The offset of the last message successfully received
        /// </summary>
        public ulong LastOffset { get; private set; }


        /// <summary>
        /// The total number of messages consumed.
        /// </summary>
        public ulong MessagesConsumed
        {
            get { return MessageCounter; }
        }



        /// <summary>
        /// How many messages have been consumed since last Checkpoint
        /// </summary>
        public int OffsetCounter { get; protected set; }


        /// <summary>
        /// How many messages need to be consumed before Saving the offset.  IF you wish more advanced, you will have to create your own check method.
        /// </summary>
        public int OffsetCheckPointLimit { get; set; } = 80;



        /// <summary>
        /// Initiate the Consumption cycle
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        public async Task ConsumeAsync()
        {
            if (_callHandler == null)
                throw new ApplicationException("Must Set the Call Handler before calling Consume method.");

            // Retrieve any saved offset
            bool offsetFound = false;
            IOffsetType offsetType;
            try
            {
                ulong priorOffset = await _streamSystem.QueryOffset(ApplicationName, _mqStreamName);
                offsetType = new OffsetTypeOffset(priorOffset);
                offsetFound = true;
            }
            catch (Exception OffsetNotFoundException)
            {
                offsetType = new OffsetTypeFirst();
            }

            _rawConsumer = await _streamSystem.CreateRawConsumer(new RawConsumerConfig(_mqStreamName)
            {
                Reference = ApplicationName,
                OffsetSpec = offsetType,
                MessageHandler = MessageHandler

            });
        }


        /// <summary>
        /// Sets the Method to be called when a message is received.
        /// </summary>
        /// <param name="callHandler"></param>
        public void SetConsumptionHandler(Func<Message, Task<bool>> callHandler) {
            _callHandler = callHandler; 
        }



        /// <summary>
        /// Performs a Offset store operation on the MQ Stream. Sets the offset to the offset of the last received message
        /// </summary>
        /// <returns></returns>
        public async Task CheckPointSet()
        {
            if (LastOffset == 0) return;

            // TODO Not sure we shouldn't lock it.  Only negative with current code is it is possible
            // that between last=LastOffset and OffsetCounter=0 LastOffset could be increased. and we would
            // have to wait until next time in this method to store that value.
            // Save last offset so we don't need to deal with thread locking.
            ulong last = LastOffset;
            await _rawConsumer.StoreOffset(last);
            OffsetCounter = 0;
            CheckPointPerformedEvent($"Checkpoint on Stream {MQStreamName} with Reference [{ApplicationName}] was set.",last);
        }



        /// <summary>
        /// This is the method that receives the messages.  It then calls the Call Handler that actually handles the messages.
        /// </summary>
        /// <param name="streamName">Name of the stream the message came in on</param>
        /// <param name="consumer">The Consumer Stream</param>
        /// <param name="msgContext"></param>
        /// <param name="message">The message from the MQ Broker</param>
        /// <returns></returns>
        private async Task MessageHandler(RawConsumer consumer, MessageContext msgContext, Message message)
        {
            //string msg = Encoding.Default.GetString(message.Data.Contents.ToArray());
            bool success = await _callHandler(message);
            if (success)
            {
                MessageCounter++;
                OffsetCounter++;
                LastOffset = msgContext.Offset;

                // TODO - Ideally this moves out of here and into a background thread that periodically checks to see if we should checkpoint.
                if (OffsetCounter >= OffsetCheckPointLimit)
                {
                    CheckPointSet();
                }

            }
            await Task.CompletedTask;
            
        }
        
    }
}