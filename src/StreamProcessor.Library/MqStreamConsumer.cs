using System.Buffers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;

namespace SlugEnt.MQStreamProcessor;

/// <summary>
///     Provides RabbitMQ Streams Consumption capabilities
/// </summary>
public class MqStreamConsumer : MQStreamBase, IMqStreamConsumer
{
    private readonly string _consumerApplicationName;

    //private Func<string, RawConsumer, MessageContext, Message, Task> _callHandler;
    private Func<Message, Task<bool>> _callHandler;
    private IConsumer                 _rawConsumer;



    /// <summary>
    ///     Creates a Stream Consumer object
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="applicationName">
    ///     This is the name of the application that owns this Stream process.
    ///     It must be unique as it is used when Checkpointing streams and is used as the Message source when creating
    ///     messages.
    /// </param>
    /// </param>
    public MqStreamConsumer(ILogger<MqStreamConsumer> logger, IServiceProvider serviceProvider) : base(logger, EnumMQStreamType.Consumer)
    {
        ILoggerFactory        loggerFactory = serviceProvider.GetService<ILoggerFactory>();
        ILogger<StreamSystem> streamLogger  = loggerFactory.CreateLogger<StreamSystem>();
        RawConsumerLogger = loggerFactory.CreateLogger<RawConsumer>();
        StreamLogger      = streamLogger;
    }


    protected ILogger<RawConsumer> RawConsumerLogger { get; set; }



    /// <summary>
    ///     Stops the consumption of messages.
    /// </summary>
    /// <returns></returns>
    public override async Task StopAsync()
    {
        // Must call Dispose.  The Client Library will re-open the closed connection and continue sending messages.
        if (_rawConsumer != null)
        {
            _rawConsumer.Close();
            _rawConsumer.Dispose();
        }

        IsConnected = false;
    }


    /// <summary>
    ///     The date time of the last Checkpoint operation.  Callers can use this if they wish to use elapsed time based
    ///     checkpoint operations
    /// </summary>
    public DateTime CheckpointLastDateTime { get; set; } = DateTime.Now;


    /// <summary>
    ///     The offset of the last time a checkpoint was performed.  Zero indicates no checkpoint has been done yet.
    /// </summary>
    public ulong CheckpointLastOffset { get; private set; }



    /// <summary>
    ///     How many messages have been consumed since last Checkpoint
    /// </summary>
    public int CheckpointOffsetCounter { get; protected set; }


    /// <summary>
    ///     How many messages need to be consumed before Saving the offset.  IF you wish more advanced, you will have to create
    ///     your own check method.
    /// </summary>
    public int CheckpointOffsetLimit { get; set; } = 20;


    /// <summary>
    ///     Performs a Offset store operation on the MQ Stream. Sets the offset to the offset of the last received message
    /// </summary>
    /// <returns></returns>
    public async Task CheckPointSetAsync()
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
        await RabbitMQ_StoreOffsetAsync(last);
        CheckpointLastOffset    = last;
        CheckpointOffsetCounter = 0;
        CheckpointLastDateTime  = DateTime.Now;

        if (EventCheckPointSaved != null)
        {
            MqStreamCheckPointEventArgs eventArgs = new();
            eventArgs.ApplicationName = ApplicationName;
            eventArgs.StreamName      = MQStreamName;
            eventArgs.CommittedOffset = last;
            OnCheckPointSaved(eventArgs);
        }
    }


    /// <summary>
    ///     Initiate the Consumption cycle
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
            offsetType = await RabbitMQQueryOffsetAsync();
        }
        catch (OffsetNotFoundException ex)
        {
            offsetType = new OffsetTypeFirst();
            _logger.LogInformation($"Log Offset not found for stream/AppName {_mqStreamName} / {_appName} .  This is okay for 1st time consumers of a stream. ");
        }

        _rawConsumer = await RabbitMQCreateRawConsumerAsync(new RawConsumerConfig(_mqStreamName)
        {
            Reference = ApplicationName, OffsetSpec = offsetType, MessageHandler = MessageHandlerAsync
        });
    }



    /// <summary>
    ///     The offset of the last message successfully received
    /// </summary>
    public ulong LastOffset { get; private set; }


    /// <summary>
    ///     The total number of messages consumed.
    /// </summary>
    public ulong MessagesConsumed => MessageCounter;



    /// <summary>
    ///     Sets the Method to be called when a message is received.
    /// </summary>
    /// <param name="callHandler"></param>
    public void SetConsumptionHandler(Func<Message, Task<bool>> callHandler) { _callHandler = callHandler; }


    /// <summary>
    ///     Decodes the actual message body from the Message object
    /// </summary>
    /// <param name="msg"></param>
    /// <returns></returns>
    public string DecodeMessage(Message msg) => Encoding.Default.GetString(msg.Data.Contents.ToArray());



    /// <summary>
    ///     This is the method that receives the messages.  It then calls the Call Handler that actually handles the messages.
    /// </summary>
    /// <param name="streamName">Name of the stream the message came in on</param>
    /// <param name="consumer">The Consumer Stream</param>
    /// <param name="msgContext"></param>
    /// <param name="message">The message from the MQ Broker</param>
    /// <returns></returns>
    protected async Task MessageHandlerAsync(RawConsumer consumer, MessageContext msgContext, Message message)
    {
        await ProcessMessageAsync(msgContext, message);
    }



    /// <summary>
    ///     Processes the received message and performs any auto checkpoints that may be needed.
    /// </summary>
    /// <param name="msgContext"></param>
    /// <param name="message"></param>
    /// <returns></returns>
    protected virtual async Task ProcessMessageAsync(MessageContext msgContext, Message message)
    {
        bool success = await _callHandler(message);
        if (success)
        {
            MessageCounter++;
            CheckpointOffsetCounter++;
            LastOffset = msgContext.Offset;


            if (CheckpointOffsetCounter >= CheckpointOffsetLimit)
                CheckPointSetAsync();
        }

        await Task.CompletedTask;
    }


    /// <summary>
    ///     Performs an Offset store operation on the MQ Stream for this application.
    /// </summary>
    /// <param name="offset"></param>
    /// <remarks>Virtual so it can be overridden in test scenarios</remarks>
    /// <returns></returns>
    protected virtual async Task RabbitMQ_StoreOffsetAsync(ulong offset) { await _rawConsumer.StoreOffset(offset); }



    /// <summary>
    ///     Creates the raw consumer object from the MQ Stream
    /// </summary>
    /// <param name="config"></param>
    /// <remarks>Virtual so it can be overridden and moq'd</remarks>
    /// <returns></returns>
    protected virtual async Task<IConsumer> RabbitMQCreateRawConsumerAsync(RawConsumerConfig config) => await _streamSystem.CreateRawConsumer(config);


    /// <summary>
    ///     Retrieves the Offset for the given application consumer and stream
    /// </summary>
    /// <remarks>Virtual so it can be overridden and moq'd</remarks>
    /// <returns></returns>
    protected virtual async Task<IOffsetType> RabbitMQQueryOffsetAsync()
    {
        ulong priorOffset = await _streamSystem.QueryOffset(ApplicationName, _mqStreamName);
        return new OffsetTypeOffset(priorOffset);
    }



    //##############################################    ########################################################
    //######################################################################################################


#region "Events"

    // Message Confirmation Error Handling
    /// <summary>
    ///     IF the OnConfirmation method is not handled by the caller, then any confirmation error will raise this event
    /// </summary>
    public event EventHandler<MqStreamCheckPointEventArgs> EventCheckPointSaved;


    protected virtual void OnCheckPointSaved(MqStreamCheckPointEventArgs e)
    {
        EventHandler<MqStreamCheckPointEventArgs> handler = EventCheckPointSaved;
        if (handler != null)
            handler(this, e);
    }

#endregion
}


/// <summary>
///     Event Args for a CheckPoint Event
/// </summary>
public class MqStreamCheckPointEventArgs : EventArgs
{
    public string ApplicationName { get; set; }
    public ulong CommittedOffset { get; set; }
    public string StreamName { get; set; }
}