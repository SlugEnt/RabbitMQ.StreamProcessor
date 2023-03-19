using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Microsoft.Extensions.DependencyInjection;

namespace SlugEnt.MQStreamProcessor
{
    /// <summary>
    /// Represents a connection to RabbitMQ Stream services.
    /// <para>Support multiple simultaneous producers and consumers.</para>
    /// </summary>
    public class MQStreamEngine : IMQStreamEngine
    {
        private readonly ILogger          _logger;
        private          IServiceProvider _serviceProvider;
        private          ILoggerFactory   _loggerFactory;



        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="serviceProvider"></param>
        /// <param name="loggerFactory"></param>
        public MQStreamEngine(ILogger<MQStreamEngine> logger, IServiceProvider serviceProvider, ILoggerFactory loggerFactory)
        {
            _logger          = logger;
            _loggerFactory   = loggerFactory;
            _serviceProvider = serviceProvider;
        }


        /// <summary>
        /// Sets / Retrieves the Stream System Config
        /// </summary>
        public StreamSystemConfig StreamSystemConfig { get; set; }

        // TODO - 



        /// <summary>
        /// Returns the dictionary of all Consumers
        /// </summary>
        public Dictionary<string, IMqStreamConsumer> StreamConsumersDictionary { get; set; } = new();



        /// <summary>
        /// Returns Dictionary of All Producers
        /// </summary>
        public Dictionary<string, IMqStreamProducer> StreamProducersDictionary { get; set; } = new();


        /// <summary>
        /// Returns a new Consumer that will access the given Stream with the application name provided.
        /// </summary>
        /// <param name="streamName">The RabbitMQ Stream to retrieve messages from</param>
        /// <param name="applicationName">The name that is provided to RabbitMQ to store Checkpoints against.</param>
        /// <param name="consumptionHandler">The name of the method that should be called to process a message whenever a message arrives.</param>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        public IMqStreamConsumer GetConsumer(string streamName, string applicationName, Func<Message, Task> consumptionHandler)
        {
            if (StreamSystemConfig == null)
                throw new ApplicationException("The StreamSystemConfig must be set before calling GetConsumer");

            ILogger<MqStreamConsumer> logConsumer      = _loggerFactory.CreateLogger<MqStreamConsumer>();
            IMqStreamConsumer         mqStreamConsumer = _serviceProvider.GetService<IMqStreamConsumer>();
            mqStreamConsumer.Initialize(streamName, applicationName, StreamSystemConfig);
            mqStreamConsumer.SetConsumptionHandler(consumptionHandler);
            StreamConsumersDictionary.Add(mqStreamConsumer.FullName, mqStreamConsumer);
            return mqStreamConsumer;
        }



        /// <summary>
        /// Returns a new Producer that will access the given Stream with the application name provided.
        /// </summary>
        /// <param name="streamName">The RabbitMQ Stream to retrieve messages from</param>
        /// <param name="applicationName">The name that is provided to RabbitMQ to store Checkpoints against.</param>
        /// <returns></returns>
        /// <exception cref="ApplicationException"></exception>
        public IMqStreamProducer GetProducer(string streamName, string applicationName)
        {
            if (StreamSystemConfig == null)
                throw new ApplicationException("The StreamSystemConfig must be set before calling GetProducer");

            ILogger<MqStreamProducer> logProducer      = _loggerFactory.CreateLogger<MqStreamProducer>();
            IMqStreamProducer         mqStreamProducer = _serviceProvider.GetService<IMqStreamProducer>();
            if (mqStreamProducer == null)
                throw new ApplicationException(
                                               "Unable to create a MqStreamProducer object.  Ensure IServiceProvider has this class available");

            mqStreamProducer.Initialize(streamName, applicationName, StreamSystemConfig);
            StreamProducersDictionary.Add(mqStreamProducer.FullName, mqStreamProducer);
            return mqStreamProducer;
        }


        /// <summary>
        /// Removes the given stream from the engine.  It will stop it first.
        /// </summary>
        /// <param name="mqStream"></param>
        /// <returns></returns>
        public async Task RemoveStreamAsync(MQStreamBase mqStream)
        {
            // Stop the consumer and remove it.
            if (mqStream.MqStreamType == EnumMQStreamType.Consumer)
            {
                MqStreamConsumer consumer = (MqStreamConsumer)mqStream;
                await consumer.StopAsync();
                StreamConsumersDictionary.Remove(consumer.FullName);
                consumer = null;
            }
        }



        /// <summary>
        /// Starts all the Consumers
        /// </summary>
        /// <returns></returns>
        public async Task StartAllConsumersAsync()
        {
            List<Task> startTasks = new List<Task>();

            foreach (KeyValuePair<string, IMqStreamConsumer> kvConsumer in StreamConsumersDictionary)
            {
                startTasks.Add(StartConsumerAsync(kvConsumer.Value));
            }

            await Task.WhenAll(startTasks);
        }


        /// <summary>
        /// Starts the provided consumer
        /// </summary>
        /// <returns></returns>
        public async Task StartConsumerAsync(IMqStreamConsumer consumer)
        {
            try
            {
                await consumer.ConnectAsync();
                await consumer.ConsumeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                throw;
            }
        }



        /// <summary>
        /// Starts all the Producers
        /// </summary>
        /// <returns></returns>
        public async Task StartAllProducersAsync()
        {
            List<Task> startTasks = new List<Task>();

            foreach (KeyValuePair<string, IMqStreamProducer> kvProducer in StreamProducersDictionary)
            {
                startTasks.Add(StartProducerAsync(kvProducer.Value));
            }

            await Task.WhenAll(startTasks);
        }



        /// <summary>
        /// Starts the given producer
        /// </summary>
        /// <param name="producer">The MQstreamProducer to startup.</param>
        /// <returns></returns>
        public async Task StartProducerAsync(IMqStreamProducer producer)
        {
            try
            {
                await producer.ConnectAsync();
                await producer.StartAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                throw;
            }
        }



        /// <summary>
        /// Starts all the producers first and then the consumers.
        /// </summary>
        /// <returns></returns>
        public async Task StartAllStreamsAsync()
        {
            await StartAllProducersAsync();
            await StartAllConsumersAsync();
        }



        /// <summary>
        /// Stops All consumers and then stops all producers.
        /// </summary>
        /// <returns></returns>
        public async Task StopAllAsync() { await StopAllConsumersAsync(); }


        /// <summary>
        /// Stops all the consumers.  Waits for all to be stopped before returning.
        /// </summary>
        /// <returns></returns>
        public async Task StopAllConsumersAsync()
        {
            List<Task> closeTasks = new List<Task>();
            foreach (KeyValuePair<string, IMqStreamConsumer> consumer in StreamConsumersDictionary)
                closeTasks.Add(consumer.Value.StopAsync());
            await Task.WhenAll(closeTasks);
        }



        /// <summary>
        /// Stops all the producers.  Waits for all to be stopped before returning.
        /// </summary>
        /// <returns></returns>
        public async Task StopAllProducers()
        {
            List<Task> closeTasks = new List<Task>();
            foreach (KeyValuePair<string, IMqStreamProducer> producer in StreamProducersDictionary)
                closeTasks.Add(producer.Value.StopAsync());
            await Task.WhenAll(closeTasks);
        }
    }
}