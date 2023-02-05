using RabbitMQ.Stream.Client;
using System.Net;

namespace SlugEnt.StreamProcessor
{
    public abstract class StreamBase
    {
        protected readonly string _name;
        protected StreamSystemConfig _config;
        protected StreamSystem _streamSystem;

        public StreamBase (string name)
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

        }


        public async Task ConnectAsync()
        {
            // Connect to the broker and create the system object
            // the entry point for the client.
            // Create it once and reuse it.
            _streamSystem = await StreamSystem.Create(_config);

            // Create the stream. It is important to put some retention policy 
            // in this case is 200000 bytes.
            await _streamSystem.CreateStream(new StreamSpec(_name)
            {
                MaxLengthBytes = 20000,
                MaxSegmentSizeBytes = 10000,
                MaxAge = new TimeSpan(0, 2, 0, 0)
            }); ;
        }

    }
}
