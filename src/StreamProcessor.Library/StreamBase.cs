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

        public StreamBase (string name, StreamSpec streamSpec = null)
        {
            _name = name;
            _name = name;

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

/*
            if (streamSpec == null)
            {
                _streamSpec = new StreamSpec(_name);
            }
            else
            {
                _streamSpec = streamSpec;
                
            }
*/
        }


        public bool IsConnected { get; private set; }
        public int MaxBytesInMb { get; set; }
        public int MaxSegmentSizeInMb { get; set; }
        public ulong MaxAgeInMinutes { get; set; }



        public async Task ConnectAsync()
        {
            if (IsConnected) return;
            if (_streamSpec == null)
                throw new ApplicationException(
                    "Must set Stream Limits prior to this call.  Call either SetNoStreamLimits or SetStreamLimits first.");

            // Connect to the broker and create the system object
            // the entry point for the client.
            // Create it once and reuse it.
            _streamSystem = await StreamSystem.Create(_config);

            // Connect to the Stream
            _streamSystem.CreateStream(_streamSpec);

            if (_streamSystem != null) IsConnected = true;
        }


        public void SetNoStreamLimits()
        {
            _streamSpec = new StreamSpec(_name);
        }


        public async Task SetStreamLimits(int maxBytesInMb = 1, int maxSegmentSizeInMb = 1, ulong maxAgeInMinutes = 60)
        {


            // Convert Values to bytes
            MaxBytesInMb = maxBytesInMb;
            MaxSegmentSizeInMb = maxSegmentSizeInMb;
            MaxAgeInMinutes = maxAgeInMinutes;

            GenerateStreamSpec();
        }


        private void GenerateStreamSpec()
        {
            if (MaxBytesInMb == 0)
                throw new ArgumentException(
                    "maxBytesInMb is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
            if (MaxSegmentSizeInMb == 0)
                throw new ArgumentException(
                    "maxSegmentSizeInMb is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");
            if (MaxAgeInMinutes == 0)
                throw new ArgumentException(
                    "maxAgeInMinutes is zero.  You need to specify values > 0 for all 3 limits or call the method SetNoStreamLimits.");

            // Generate Raw values
            ulong maxLengthInBytes = (ulong)MaxBytesInMb * 1024 * 1024;
            int maxSegmentSizeInBytes = MaxSegmentSizeInMb * 1024 * 1024;
            TimeSpan maxAge = TimeSpan.FromMinutes(MaxAgeInMinutes);

            _streamSpec = new StreamSpec(_name)
            {
                MaxAge = maxAge,
                MaxLengthBytes = maxLengthInBytes,
                MaxSegmentSizeBytes = maxSegmentSizeInBytes
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
