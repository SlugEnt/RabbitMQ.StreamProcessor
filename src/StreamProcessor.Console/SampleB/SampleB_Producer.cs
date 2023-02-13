using SlugEnt.StreamProcessor;
using System.ComponentModel;

namespace StreamProcessor.Console.SampleB;

public class SampleB_Producer : MqStreamProducer
{
    //public Func<SampleA_Producer, BackgroundWorker, Task> ProduceMessageMethod;
    private BackgroundWorker _backgroundWorkerProducer;
    private Func<SampleB_Producer, BackgroundWorker, Task> _produceMessagesMethod;



    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="mqStreamName"></param>
    /// <param name="appName"></param>
    /// <param name="produceMessagesMethod">Method that should be called to produce messages</param>
    public SampleB_Producer(string mqStreamName, string appName,
        Func<SampleB_Producer, BackgroundWorker, Task> produceMessagesMethod) : base(mqStreamName, appName)
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

        // Setup the background worker that produces messages
        _backgroundWorkerProducer = new BackgroundWorker();
        _backgroundWorkerProducer.DoWork += BackgroundDoWork;
        _backgroundWorkerProducer.RunWorkerCompleted += ProducerCompleted;
        _backgroundWorkerProducer.WorkerReportsProgress = false;
        _backgroundWorkerProducer.WorkerSupportsCancellation = true;
        _backgroundWorkerProducer.RunWorkerAsync();
    }


    public async Task Stop()
    {
        _backgroundWorkerProducer.CancelAsync();

        // Print Final Totals
        System.Console.WriteLine("Messages:");
        System.Console.WriteLine($"  Produced:    {MessageCounter}");
    }



    /// <summary>
    /// Initiates the act of producing messages on the background thread
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    private void BackgroundDoWork(object sender, DoWorkEventArgs e)
    {
        BackgroundWorker worker = sender as BackgroundWorker;
        ProduceMessages(worker);

        e.Cancel = true;
    }



    /// <summary>
    /// Calls the method to produce messages.  That method does not return until done.
    /// </summary>
    /// <param name="worker"></param>
    private void ProduceMessages(BackgroundWorker worker)
    {
        _produceMessagesMethod(this, worker);
    }


    /// <summary>
    /// Called when the Producer has finished or after Cancellation by user was accepted
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    private void ProducerCompleted(object sender, RunWorkerCompletedEventArgs e)
    {
        if (e.Cancelled) System.Console.WriteLine("Producer was cancelled");
        else if (e.Error != null) System.Console.WriteLine("Producer had an error - {0}", e.Error.Message);
        else System.Console.WriteLine("Producer finished sending messages successfully");
    }

}