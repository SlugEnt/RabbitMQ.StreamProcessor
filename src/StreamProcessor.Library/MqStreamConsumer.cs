
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace SlugEnt.StreamProcessor
{
    public interface IMqStreamConsumer
    {
        /// <summary>
        /// The offset of the last message successfully received
        /// </summary>
        ulong LastOffset { get; }

        /// <summary>
        /// The offset of the last time a checkpoint was performed.  Zero indicates no checkpoint has been done yet.
        /// </summary>
        ulong CheckpointLastOffset { get; }

        /// <summary>
        /// The total number of messages consumed.
        /// </summary>
        ulong MessagesConsumed { get; }

        /// <summary>
        /// How many messages have been consumed since last Checkpoint
        /// </summary>
        int CheckpointOffsetCounter { get; }

        /// <summary>
        /// How many messages need to be consumed before Saving the offset.  IF you wish more advanced, you will have to create your own check method.
        /// </summary>
        int CheckpointOffsetLimit { get; set; }

        /// <summary>
        /// The date time of the last Checkpoint operation.  Callers can use this if they wish to use elapsed time based checkpoint operations
        /// </summary>
        DateTime CheckpointLastDateTime { get; set; }

        /// <summary>
        /// The application that owns this Stream Process.
        /// It is used when checkpointing the Stream and is tagged in the message properties when creating the message
        /// </summary>
        string ApplicationName { get; }

        /// <summary>
        /// The name of the stream we publish and consume messages from
        /// </summary>
        string MQStreamName { get; }

        /// <summary>
        /// Number of messages published or consumed depending on type of stream
        /// </summary>
        ulong MessageCounter { get; }

        /// <summary>
        /// Whether this stream is a publisher or consumer
        /// </summary>
        EnumMQStreamType MqStreamType { get; }

        /// <summary>
        /// Whether the stream is connected
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Maximum length this stream can be.  Only applicable on newly published streams
        /// </summary>
        ulong MaxLength { get; set; }

        /// <summary>
        /// Maximum segment size for this stream
        /// </summary>
        int MaxSegmentSize { get; set; }

        /// <summary>
        /// Max Age of records in seconds
        /// </summary>
        ulong MaxAge { get; set; }

        /// <summary>
        /// Initiate the Consumption cycle
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        Task ConsumeAsync();

        /// <summary>
        /// Sets the Method to be called when a message is received.
        /// </summary>
        /// <param name="callHandler"></param>
        void SetConsumptionHandler(Func<Message, Task<bool>> callHandler);

        /// <summary>
        /// Performs a Offset store operation on the MQ Stream. Sets the offset to the offset of the last received message
        /// </summary>
        /// <returns></returns>
        Task CheckPointSet();

        /// <summary>
        /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
        /// </summary>
        event EventHandler<MqStreamCheckPointEventArgs> EventCheckPointSaved;

        /// <summary>
        /// Initializes the Stream
        /// </summary>
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// <param name="mqStreamType">The type of MQ Stream</param>
        void Initialize(string mqStreamName, string applicationName,StreamSystemConfig config);

        /// <summary>
        /// Establishes a connection to the stream on the RabbitMQ server(s).
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        Task ConnectAsync();

        /// <summary>
        /// Permanently deletes the Stream off the RabbitMQ Servers.
        /// </summary>
        /// <returns></returns>
        Task DeleteStream();

        Task StreamInfo();
    }

    /// <summary>
    /// Provides RabbitMQ Streams Consumption capabilities
    /// </summary>
    public class MqStreamConsumer : MQStreamBase, IMqStreamConsumer
    {
        //private Func<string, RawConsumer, MessageContext, Message, Task> _callHandler;
        private Func<Message, Task<bool>> _callHandler;
        private readonly string _consumerApplicationName;
        private IConsumer _rawConsumer;



        /// <summary>
        /// Creates a Stream Consumer object
        /// </summary>
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// </param>
        public MqStreamConsumer(ILogger<MqStreamConsumer> logger) : base(logger, EnumMQStreamType.Consumer)
        {

        }


        /// <summary>
        /// The offset of the last message successfully received
        /// </summary>
        public ulong LastOffset { get; private set; } = 0;


        /// <summary>
        /// The offset of the last time a checkpoint was performed.  Zero indicates no checkpoint has been done yet.
        /// </summary>
        public ulong CheckpointLastOffset { get; private set; } = 0;


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
        public int CheckpointOffsetCounter { get; protected set; } = 0;


        /// <summary>
        /// How many messages need to be consumed before Saving the offset.  IF you wish more advanced, you will have to create your own check method.
        /// </summary>
        public int CheckpointOffsetLimit { get; set; } = 20;


        /// <summary>
        /// The date time of the last Checkpoint operation.  Callers can use this if they wish to use elapsed time based checkpoint operations
        /// </summary>
        public DateTime CheckpointLastDateTime { get; set; } = DateTime.Now;


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
                    offsetType = await RabbitMQQueryOffset();
                }
                catch (OffsetNotFoundException ex )
                {
                    offsetType = new OffsetTypeFirst();
                    _logger.LogInformation($"Log Offset not found for stream/AppName {_mqStreamName} / {_appName} .  This is okay for 1st time consumers of a stream. ");
                }
            
            _rawConsumer = await RabbitMQCreateRawConsumer(new RawConsumerConfig(_mqStreamName)
            {
                Reference = ApplicationName,
                OffsetSpec = offsetType,
                MessageHandler = MessageHandler
            });
        }


        /// <summary>
        /// Retrieves the Offset for the given application consumer and stream
        /// </summary>
        /// <remarks>Virtual so it can be overridden and moq'd</remarks>
        /// <returns></returns>
        protected virtual async Task<IOffsetType> RabbitMQQueryOffset()
        {
            ulong priorOffset = await _streamSystem.QueryOffset(ApplicationName, _mqStreamName);
            return new OffsetTypeOffset(priorOffset);
        }



        /// <summary>
        /// Creates the raw consumer object from the MQ Stream
        /// </summary>
        /// <param name="config"></param>
        /// <remarks>Virtual so it can be overridden and moq'd</remarks>
        /// <returns></returns>
        protected virtual async Task<IConsumer> RabbitMQCreateRawConsumer(RawConsumerConfig config)
        {
            return await _streamSystem.CreateRawConsumer(config);
        }



        /// <summary>
        /// Sets the Method to be called when a message is received.
        /// </summary>
        /// <param name="callHandler"></param>
        public void SetConsumptionHandler(Func<Message, Task<bool>> callHandler) {
            _callHandler = callHandler; 
        }


        /// <summary>
        /// Performs an Offset store operation on the MQ Stream for this application.
        /// </summary>
        /// <param name="offset"></param>
        /// <remarks>Virtual so it can be overridden in test scenarios</remarks>
        /// <returns></returns>
        protected virtual async Task RabbitMQ_StoreOffset(ulong offset)
        {
            await _rawConsumer.StoreOffset(offset);
        }


        /// <summary>
        /// Performs a Offset store operation on the MQ Stream. Sets the offset to the offset of the last received message
        /// </summary>
        /// <returns></returns>
        public async Task CheckPointSet()
        {
            // If no messages have been consumed, then store date time and return
            if (LastOffset == 0)
            {
                CheckpointLastDateTime = DateTime.Now;
                return;
            }

            // TODO Not sure we shouldn't lock it.  Only negative with current code is it is possible
            // that between last=LastOffset and CheckpointOffsetCounter=0 LastOffset could be increased. and we would
            // have to wait until next time in this method to store that value.
            // Save last offset so we don't need to deal with thread locking.
            ulong last = LastOffset;
            await RabbitMQ_StoreOffset(last);
            CheckpointLastOffset = last;
            CheckpointOffsetCounter = 0;
            CheckpointLastDateTime = DateTime.Now;

            if (EventCheckPointSaved != null)
            {
                MqStreamCheckPointEventArgs eventArgs = new MqStreamCheckPointEventArgs();
                eventArgs.ApplicationName = ApplicationName;
                eventArgs.StreamName = MQStreamName;
                eventArgs.CommittedOffset = last;
                OnCheckPointSaved(eventArgs);
            }
        }



        /// <summary>
        /// This is the method that receives the messages.  It then calls the Call Handler that actually handles the messages.
        /// </summary>
        /// <param name="streamName">Name of the stream the message came in on</param>
        /// <param name="consumer">The Consumer Stream</param>
        /// <param name="msgContext"></param>
        /// <param name="message">The message from the MQ Broker</param>
        /// <returns></returns>
        protected async Task MessageHandler(RawConsumer consumer, MessageContext msgContext, Message message)
        {
            await ProcessMessage(msgContext,message);
        }


        protected virtual async Task ProcessMessage(MessageContext msgContext, Message message)
        {
            bool success = await _callHandler(message);
            if (success)
            {
                MessageCounter++;
                CheckpointOffsetCounter++;
                LastOffset = msgContext.Offset;


                if (CheckpointOffsetCounter >= CheckpointOffsetLimit)
                {
                    CheckPointSet();
                }

            }
            await Task.CompletedTask;
        }


        /// <summary>
        /// Decodes the actual message bode from the Message object
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public string DecodeMessage(Message msg)
        {
            return Encoding.Default.GetString(msg.Data.Contents.ToArray());
        }



        //##############################################    ########################################################
        //######################################################################################################
        #region "Events"
        // Message Confirmation Error Handling
        /// <summary>
        /// IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
        /// </summary>
        public event EventHandler<MqStreamCheckPointEventArgs> EventCheckPointSaved;

        protected virtual void OnCheckPointSaved(MqStreamCheckPointEventArgs e)
        {
            EventHandler<MqStreamCheckPointEventArgs> handler = EventCheckPointSaved;
            if (handler != null) handler(this, e);
        }



        #endregion

    }


    /// <summary>
    /// Event Args for a CheckPoint Event
    /// </summary>
    public class MqStreamCheckPointEventArgs : EventArgs
    {
        public ulong CommittedOffset { get; set; }
        public string ApplicationName { get; set; }
        public string StreamName { get; set; }
    }

}
