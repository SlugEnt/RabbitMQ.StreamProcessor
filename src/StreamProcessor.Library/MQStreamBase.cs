using System.ComponentModel.DataAnnotations;
using RabbitMQ.Stream.Client;
using System.Net;
using Microsoft.Extensions.Logging;

namespace SlugEnt.StreamProcessor
{
    /// <summary>
    /// The base for the MQStreamProducer and MQStreamConsumer classes
    /// </summary>
    public abstract class MQStreamBase
    {
        protected string _mqStreamName = "";
        protected StreamSystemConfig _config;
        protected StreamSystem _streamSystem;
        protected StreamSpec _streamSpec;
        protected string _appName;
        protected ILogger<MQStreamBase> _logger;


        public MQStreamBase(ILogger<MQStreamBase> iLogger, EnumMQStreamType mqStreamType)
        {
            _logger = iLogger;
            MqStreamType = mqStreamType;
        }



        /// <summary>
        /// Initializes the Stream
        /// </summary>
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// <param name="mqStreamType">The type of MQ Stream</param>
        public void Initialize(string mqStreamName, string applicationName, StreamSystemConfig streamConfig)
        {
            if (_mqStreamName != string.Empty) throw new ArgumentException("A Stream can only be initialized once.");

            _mqStreamName = mqStreamName;
            
            _appName = applicationName;

            if (applicationName == string.Empty)
                throw new ArgumentException(
                    "The ApplicationName must be specified and it must be unique for a given application");


            _config = streamConfig;

        }





        /// <summary>
        /// The application that owns this Stream Process.
        /// It is used when checkpointing the Stream and is tagged in the message properties when creating the message
        /// </summary>
        public string ApplicationName
        {
            get { return _appName; }
        }

        /// <summary>
        /// The name of the stream we publish and consume messages from
        /// </summary>
        public string MQStreamName
        {
            get { return _mqStreamName; }
        }


        /// <summary>
        /// Number of messages published or consumed depending on type of stream
        /// </summary>
        public ulong MessageCounter { get; protected set; } = 0;

        /// <summary>
        /// Whether this stream is a publisher or consumer
        /// </summary>
        public EnumMQStreamType MqStreamType { get; private set; }


        /// <summary>
        /// Whether the stream is connected
        /// </summary>
        public bool IsConnected { get; protected set; } = false;

        /// <summary>
        /// Maximum length this stream can be.  Only applicable on newly published streams
        /// </summary>
        public ulong MaxLength { get; set; } = 0;

        /// <summary>
        /// Maximum segment size for this stream
        /// </summary>
        public int MaxSegmentSize { get; set; } = 0;

        /// <summary>
        /// Max Age of records in seconds
        /// </summary>
        public ulong MaxAge { get; set; } = 0;



        /// <summary>
        /// Establishes a connection to the stream on the RabbitMQ server(s).
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        public async Task ConnectAsync()
        {
            if (IsConnected) return;

            // Connect to the broker and create the system object
            // the entry point for the client.
            // Create it once and reuse it.
            _streamSystem = await StreamSystem.Create(_config);


            // See if we need Stream Specs.  If it already exists on server we do not.
            bool streamExists = await RabbitMQ_StreamExists(_mqStreamName);
            if (! streamExists)
            {
                if (MqStreamType == EnumMQStreamType.Consumer)
                    // TODO =- Change To Some type of Stream Exception
                    throw new StreamSystemInitialisationException("Stream - " + _mqStreamName + " does not exist.");
                else if (_streamSpec == null)
                    throw new StreamSystemInitialisationException(
                        "For new Producer Streams you must set Stream Limits prior to this call.  Call either SetNoStreamLimits or SetStreamLimits first.");

                // Connect to the Stream
                _streamSystem.CreateStream(_streamSpec);
            }
            else
                // Connect to the Stream - But use the existing definition
                _streamSystem.CreateStream(null);

            if (_streamSystem != null) IsConnected = true;
        }



        /// <summary>
        /// Calls RabbitMQ to see if the stream exists on the server
        /// </summary>
        /// <param name="streamName"></param>
        /// <returns></returns>
        protected virtual async Task<bool> RabbitMQ_StreamExists(string streamName)
        {
            return await _streamSystem.StreamExists(_mqStreamName);
        }


        /// <summary>
        /// Permanently deletes the Stream off the RabbitMQ Servers.
        /// </summary>
        /// <returns></returns>
        public async Task DeleteStream()
        {
            await _streamSystem.DeleteStream(_mqStreamName);
        }

        public async Task StreamInfo()
        {
         
        }


        // These are Message.ApplicationProperties Keys that are used
        public const string AP_RETRIES = "Retries";

    }
}
