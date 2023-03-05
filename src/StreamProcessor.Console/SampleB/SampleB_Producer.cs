using SlugEnt.StreamProcessor;
using System.ComponentModel;
using Microsoft.Extensions.Logging;
using StreamProcessor.Console.SampleA;

namespace StreamProcessor.Console.SampleB;

public interface ISampleB_Producer : IMqStreamProducer
{
    public Task Start();
    public Task Stop();
    public void SetProducerMethod(Func<SampleB_Producer, Task> method);
}


public class SampleB_Producer : MqStreamProducer, IMqStreamProducer, ISampleB_Producer
{
    private Func<SampleB_Producer, Task> _produceMessagesMethod;
    private Thread _workerThread;



    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="appName"></param>
    /// <param name="produceMessagesMethod">Method that should be called to produce messages</param>
    public SampleB_Producer(ILogger<SampleB_Producer> logger, IServiceProvider serviceProvider) : base(logger, serviceProvider)
    {
        //_produceMessagesMethod = produceMessagesMethod;
    }



    public void SetProducerMethod(Func<SampleB_Producer, Task> method)
    {
        _produceMessagesMethod = method;
    }



    /// <summary>
    /// Initiates the startup of the Producer, establishes connection to RabbitMQ
    /// </summary>
    /// <returns></returns>
    public async Task Start()
    {
        await ConnectAsync();
        await StartAsync();

        _workerThread = new Thread(ProduceMessages);
        _workerThread.Start();
    }


    public async Task Stop()
    {
        IsCancelled = true;
        
        // Print Final Totals
        System.Console.WriteLine("Messages:");
        System.Console.WriteLine($"  Produced:    {MessageCounter}");
    }





    /// <summary>
    /// Calls the method to produce messages.  That method does not return until done.
    /// </summary>
    /// <param name="worker"></param>
    private void ProduceMessages()
    {
        _produceMessagesMethod(this);
    }




    /// <summary>
    /// When set to true the producing method from the caller should stop processing
    /// </summary>
    public bool IsCancelled { get; protected set; }
}