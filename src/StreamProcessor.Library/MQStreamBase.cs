using ByteSizeLib;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;

namespace SlugEnt.MQStreamProcessor;

/// <summary>
///     The base for the MQStreamProducer and MQStreamConsumer classes
/// </summary>
public abstract class MQStreamBase
{
    // These are Message.ApplicationProperties Keys that are used
    public const string                AP_RETRIES = "Retries";
    protected    string                _appName;
    protected    StreamSystemConfig    _config;
    protected    ILogger<MQStreamBase> _logger;
    protected    string                _mqStreamName = "";
    protected    StreamSpec            _streamSpec;
    protected    StreamSystem          _streamSystem;


    public MQStreamBase(ILogger<MQStreamBase> iLogger, EnumMQStreamType mqStreamType)
    {
        _logger      = iLogger;
        MqStreamType = mqStreamType;
    }



    /// <summary>
    ///     The application that owns this Stream Process.
    ///     It is used when checkpointing the Stream and is tagged in the message properties when creating the message
    /// </summary>
    public string ApplicationName => _appName;


    /// <summary>
    ///     Builds the Fullname for this MQStream.
    ///     <para>Fullname is stream name combined with application name</para>
    ///     ">
    /// </summary>
    public string FullName => MQStreamName + "." + ApplicationName;


    /// <summary>
    ///     Whether the stream is connected
    /// </summary>
    public bool IsConnected { get; protected set; }


    /// <summary>
    ///     True if the Stream has been initialized
    /// </summary>
    public bool IsInitialized { get; protected set; }

    /// <summary>
    ///     Max Age of records in seconds
    /// </summary>
    public TimeSpan MaxAge { get; set; }


    /// <summary>
    ///     Maximum segment size for this stream
    /// </summary>
    public ByteSize MaxSegmentSize { get; set; }

    /// <summary>
    ///     Maximum length this stream can be.  Only applicable on newly published streams
    /// </summary>

    //public ulong MaxStreamSize { get; set; } = 0;

    public ByteSize MaxStreamSize { get; set; }



    /// <summary>
    ///     Number of messages published or consumed depending on type of stream
    /// </summary>
    public ulong MessageCounter { get; protected set; } = 0;

    /// <summary>
    ///     The name of the stream we publish and consume messages from
    /// </summary>
    public string MQStreamName => _mqStreamName;

    /// <summary>
    ///     Whether this stream is a publisher or consumer
    /// </summary>
    public EnumMQStreamType MqStreamType { get; }


    /// <summary>
    ///     Sets the stream system logger
    /// </summary>
    public ILogger<StreamSystem> StreamLogger { get; protected set; } = null;



    /// <summary>
    ///     Closes the connection to MQ.
    /// </summary>
    protected async Task CloseStreamAsync()
    {
        // Delete the stream and disconnect.
        await _streamSystem.Close();
        IsConnected = false;
    }



    /// <summary>
    ///     Establishes a connection to the stream on the RabbitMQ server(s).
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    public virtual async Task ConnectAsync()
    {
        if (IsConnected)
            return;

        try
        {
            _streamSystem = await StreamSystem.Create(_config, StreamLogger);


            // See if we need Stream Specs.  If it already exists on server we do not.
            bool streamExists = await RabbitMQ_StreamExistsAsync(_mqStreamName);
            if (!streamExists)
            {
                if (MqStreamType == EnumMQStreamType.Consumer)
                    throw new StreamSystemInitialisationException("Stream - " + _mqStreamName + " does not exist.");
                if (_streamSpec == null)
                    throw new StreamSystemInitialisationException(
                                                                  "For new Producer Streams you must set Stream Limits prior to this call.  Call either SetNoStreamLimits or SetStreamLimitsAsync first.");

                // Connect to the Stream
                _streamSystem.CreateStream(_streamSpec);
            }
            else

                // Connect to the Stream - But use the existing definition
            {
                _streamSystem.CreateStream(null);
            }

            if (_streamSystem != null)
                IsConnected = true;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error connecting to RabbitMQ Stream - {ex.Message}");
        }
    }


    /// <summary>
    ///     Permanently deletes the Stream off the RabbitMQ Servers.
    /// </summary>
    /// <returns></returns>
    public async Task DeleteStreamFromRabbitMQ()
    {
        try
        {
            // Create the stream if it does not exist.
            if (_streamSystem == null)
                _streamSystem = await StreamSystem.Create(_config, StreamLogger);


            if (!await RabbitMQ_StreamExistsAsync(_mqStreamName))
                return;


            // Delete
            await _streamSystem.DeleteStream(_mqStreamName);
        }
        catch (Exception e)
        {
            _logger.LogError($"Error attempting to delete stream {_mqStreamName}.  {e.Message}", e);
        }
    }



    /// <summary>
    ///     Initializes the Stream
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="applicationName">
    ///     This is the name of the application that owns this Stream process.
    ///     It must be unique as it is used when Checkpointing streams and is used as the Message source when creating
    ///     messages.
    /// </param>
    /// <param name="mqStreamType">The type of MQ Stream</param>
    public void Initialize(string mqStreamName, string applicationName, StreamSystemConfig streamConfig)
    {
        if (_mqStreamName != string.Empty)
            throw new ArgumentException("A Stream can only be initialized once.");

        _mqStreamName = mqStreamName;

        _appName = applicationName;

        if (applicationName == string.Empty)
            throw new ArgumentException(
                                        "The ApplicationName must be specified and it must be unique for a given application");

        _config       = streamConfig;
        IsInitialized = true;
    }



    /// <summary>
    ///     Calls RabbitMQ to see if the stream exists on the server
    /// </summary>
    /// <param name="streamName"></param>
    /// <returns></returns>
    protected virtual async Task<bool> RabbitMQ_StreamExistsAsync(string streamName) => await _streamSystem.StreamExists(_mqStreamName);


    /// <summary>
    ///     Stops the producer / stream.
    /// </summary>
    /// <returns></returns>
    public abstract Task StopAsync();



    public async Task StreamInfo() { }
}