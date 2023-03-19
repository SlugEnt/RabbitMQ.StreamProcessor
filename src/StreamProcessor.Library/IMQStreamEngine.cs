using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;

namespace SlugEnt.MQStreamProcessor;

public interface IMQStreamEngine
{
    /// <summary>
    /// Returns a new Producer that will access the given Stream with the application name provided.
    /// </summary>
    /// <param name="streamName">The RabbitMQ Stream to retrieve messages from</param>
    /// <param name="applicationName">The name that is provided to RabbitMQ to store Checkpoints against.</param>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    IMqStreamProducer GetProducer(string streamName, string applicationName);


    /// <summary>
    /// Sets / Retrieves the Stream System Config
    /// </summary>
    StreamSystemConfig StreamSystemConfig { get; set; }

    /// <summary>
    /// Returns the dictionary of all Consumers
    /// </summary>
    Dictionary<string, IMqStreamConsumer> StreamConsumersDictionary { get; set; }

    /// <summary>
    /// Returns Dictionary of All Producers
    /// </summary>
    Dictionary<string, IMqStreamProducer> StreamProducersDictionary { get; set; }


    /// <summary>
    /// Returns a new Consumer that will access the given Stream with the application name provided.
    /// </summary>
    /// <param name="streamName">The RabbitMQ Stream to retrieve messages from</param>
    /// <param name="applicationName">The name that is provided to RabbitMQ to store Checkpoints against.</param>
    /// <param name="consumptionHandler">The name of the method that should be called to process a message whenever a message arrives.</param>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    IMqStreamConsumer GetConsumer(string streamName, string applicationName, Func<Message, Task> consumptionHandler);


    Task StartAllConsumersAsync();


    /// <summary>
    /// Starts the consumer
    /// </summary>
    /// <returns></returns>
    Task StartConsumerAsync(IMqStreamConsumer consumer);


    /// <summary>
    /// Starts the given producer
    /// </summary>
    /// <param name="producer">The MQstreamProducer to startup.</param>
    /// <returns></returns>
    Task StartProducerAsync(IMqStreamProducer producer);


    /// <summary>
    /// Stops All consumers and then stops all producers.
    /// </summary>
    /// <returns></returns>
    Task StopAllAsync();


    /// <summary>
    /// Stops all the consumers.  Waits for all to be stopped before returning.
    /// </summary>
    /// <returns></returns>
    Task StopAllConsumersAsync();


    /// <summary>
    /// Stops all the producers.  Waits for all to be stopped before returning.
    /// </summary>
    /// <returns></returns>
    Task StopAllProducers();


    /// <summary>
    /// Starts all the Producers
    /// </summary>
    /// <returns></returns>
    Task StartAllProducersAsync();


    /// <summary>
    /// Starts all the consumers first and then the producers.
    /// </summary>
    /// <returns></returns>
    Task StartAllStreamsAsync();
}