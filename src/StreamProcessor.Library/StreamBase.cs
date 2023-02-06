using System.ComponentModel.DataAnnotations;
using RabbitMQ.Stream.Client;
using System.Net;

namespace SlugEnt.StreamProcessor
{
    public abstract class StreamBase
    {
        protected readonly string _name;
        protected StreamSystemConfig _config;
        protected StreamSystem _streamSystem;
        protected StreamSpec _streamSpec;


        public StreamBase (string name, EnumStreamType streamType)
        {
            _name = name;
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

        public EnumStreamType StreamType { get; private set; }
        public bool IsConnected { get; private set; }
        public ulong MaxLength { get; set; }
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
            if (!await _streamSystem.StreamExists(_name))
            {
                if (StreamType == EnumStreamType.Consumer)
                    throw new ApplicationException("Stream - " + _name + " does not exist.");
                if (_streamSpec == null)
                    throw new ApplicationException(
                        "For new Streams you must set Stream Limits prior to this call.  Call either SetNoStreamLimits or SetStreamLimits first.");
            }

            // Connect to the Stream
            _streamSystem.CreateStream(_streamSpec);

            if (_streamSystem != null) IsConnected = true;
        }


        /// <summary>
        /// Sets no limits for the stream - It will either be controlled by RabbitMQ policies or have no limits - which is unadvisable.
        /// </summary>
        public void SetNoStreamLimits()
        {
            _streamSpec = new StreamSpec(_name);
        }


        /// <summary>
        /// Sets the stream specifications in its raw RabbitMQ requested units of measure
        /// </summary>
        /// <param name="maxLength"></param>
        /// <param name="maxSegmentSize"></param>
        /// <param name="maxAgeInSeconds"></param>
        /// <returns></returns>
        public async Task SetStreamLimitsRaw(ulong maxLength, int maxSegmentSize, ulong maxAgeInSeconds)
        {
            MaxAge = maxAgeInSeconds;
            MaxLength = maxLength;
            MaxSegmentSize = maxSegmentSize;
            GenerateStreamSpec();
        }



        /// <summary>
        /// Sets the Stream Limits in more typical units of measure
        /// </summary>
        /// <param name="maxBytesInMb"></param>
        /// <param name="maxSegmentSizeInMb"></param>
        /// <param name="maxAgeInHours"></param>
        /// <returns></returns>
        public async Task SetStreamLimits(int maxBytesInMb = 1, int maxSegmentSizeInMb = 1, ulong maxAgeInHours = 24)
        {
            // Convert Values to bytes
            MaxLength = (ulong)maxBytesInMb * 1024 * 1024;
            MaxSegmentSize = maxSegmentSizeInMb * 1024 * 1024;
            MaxAge = maxAgeInHours * 24 * 60 * 60;

            GenerateStreamSpec();
        }



        /// <summary>
        /// Creates the Stream Specifications - Max age of messages, Max size of the stream, and the max size of a segment file
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        private void GenerateStreamSpec()
        {
            if (MaxLength == 0)
                throw new ArgumentException(
                    "maxLength is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
            if (MaxSegmentSize == 0)
                throw new ArgumentException(
                    "maxSegmentSize is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
            if (MaxAge == 0)
                throw new ArgumentException(
                    "maxAge is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");


            TimeSpan maxAge = TimeSpan.FromSeconds(MaxAge);

            _streamSpec = new StreamSpec(_name)
            {
                MaxAge = maxAge,
                MaxLengthBytes = MaxLength,
                MaxSegmentSizeBytes = MaxSegmentSize
            };
        }


        /// <summary>
        /// This is temporary so I can continue to use my sample queue.  
        /// </summary>
        /// <returns></returns>
        public async Task SetStreamLimitsSmall()
        {
            _streamSpec = new StreamSpec(_name)
            {
                MaxAge = TimeSpan.FromHours(2),
                MaxLengthBytes = 20000,
                MaxSegmentSizeBytes = 10000
            };
        }


        /// <summary>
        /// Permanently deletes the Stream off the RabbitMQ Servers.
        /// </summary>
        /// <returns></returns>
        public async Task DeleteStream()
        {
            await _streamSystem.DeleteStream(_name);
        }
    }
}
