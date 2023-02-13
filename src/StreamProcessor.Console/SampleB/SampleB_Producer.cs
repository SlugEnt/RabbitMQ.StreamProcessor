using SlugEnt.StreamProcessor;
using System.ComponentModel;

namespace StreamProcessor.Console.SampleB;

public class SampleB_Producer : MqStreamProducer
{
    private Func<SampleB_Producer, Task> _produceMessagesMethod;
    private Thread _workerThread;



    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="appName"></param>
    /// <param name="produceMessagesMethod">Method that should be called to produce messages</param>
    public SampleB_Producer(string mqStreamName, string appName,
        Func<SampleB_Producer, Task> produceMessagesMethod) : base(mqStreamName, appName)
    {
        _produceMessagesMethod = produceMessagesMethod;
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