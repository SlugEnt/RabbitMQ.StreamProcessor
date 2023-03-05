using RabbitMQ.Stream.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SlugEnt.StreamProcessor
{
    public interface IMQStreamBase
    {
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
        /// Returns the Fullname for this MQStream.
        /// <para>Fullname is stream name combined with application name</para>">
        /// </summary>
        public string FullName { get; }


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
        /// Establishes a connection to the stream on the RabbitMQ server(s).
        /// </summary>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        Task ConnectAsync();


        /// <summary>
        /// Closes the connection to MQ.
        /// </summary>
        public Task StopAsync();


        /// <summary>
        /// Initializes the Stream
        /// </summary>
        /// <param name="mqStreamName"></param>
        /// <param name="applicationName">This is the name of the application that owns this Stream process.
        /// It must be unique as it is used when Checkpointing streams and is used as the Message source when creating messages.</param>
        /// <param name="mqStreamType">The type of MQ Stream</param>
        void Initialize(string mqStreamName, string applicationName, StreamSystemConfig config);


        /// <summary>
        /// Permanently deletes the Stream off the RabbitMQ Servers.
        /// </summary>
        /// <returns></returns>
        Task DeleteStreamFromRabbitMQ();

        Task StreamInfo();

    }
}
