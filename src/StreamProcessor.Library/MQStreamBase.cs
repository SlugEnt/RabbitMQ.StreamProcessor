using System.ComponentModel.DataAnnotations;
using RabbitMQ.Stream.Client;
using System.Net;

namespace SlugEnt.StreamProcessor
{
    public abstract class StreamBase
    {
        protected readonly string _streamName;
        protected StreamSystemConfig _config;
        protected StreamSystem _streamSystem;
        protected StreamSpec _streamSpec;


        public StreamBase (string streamName, EnumStreamType streamType)
        {
            _streamName = streamName;
            StreamType = streamType;
            

            IPEndPoint a = Helpers.GetIPEndPointFromHostName("rabbitmqa.slug.local", 5552);
            IPEndPoint b = Helpers.GetIPEndPointFromHostName("rabbitmqb.slug.local", 5552);
            IPEndPoint c = Helpers.GetIPEndPointFromHostName("rabbitmqc.slug.local", 5552);

            _config = new StreamSystemConfig
            {
                UserName = "testUser",
                Password = "TESTUSER",
                VirtualHost = "Test",
                Endpoints = new List<EndPoint> {
                    a,b,c
                },
            };

        }


        /// <summary>
        /// The name of the stream we publish and consume messages from
        /// </summary>
        public string StreamName
        {
            get { return _streamName; }
        }


        /// <summary>
        /// Number of messages published or consumed depending on type of stream
        /// </summary>
        public ulong MessageCounter { get; protected set; } = 0;

        /// <summary>
        /// Whether this stream is a publisher or consumer
        /// </summary>
        public EnumStreamType StreamType { get; private set; }


        /// <summary>
        /// Whether the stream is connected
        /// </summary>
        public bool IsConnected { get; private set; }

        /// <summary>
        /// Maximum length this stream can be.  Only applicable on newly published streams
        /// </summary>
        public ulong MaxLength { get; set; }

        /// <summary>
        /// Maximum segment size for this stream
        /// </summary>
        public int MaxSegmentSize { get; set; }

        /// <summary>
        /// Max Age of records in seconds
        /// </summary>
        public ulong MaxAge { get; set; }



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
            if (!await _streamSystem.StreamExists(_streamName))
            {
                if (StreamType == EnumStreamType.Consumer)
                    // TODO =- Change To Some type of Stream Exception
                    throw new  ApplicationException("Stream - " + _streamName + " does not exist.");
                if (_streamSpec == null)
                    throw new ApplicationException(
                        "For new Streams you must set Stream Limits prior to this call.  Call either SetNoStreamLimits or SetStreamLimits first.");
            }

            // Connect to the Stream
            _streamSystem.CreateStream(_streamSpec);

            if (_streamSystem != null) IsConnected = true;
        }



        /// <summary>
        /// Permanently deletes the Stream off the RabbitMQ Servers.
        /// </summary>
        /// <returns></returns>
        public async Task DeleteStream()
        {
            await _streamSystem.DeleteStream(_streamName);
        }

        public async Task StreamInfo()
        {
         
        }
    }
}
