using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Text;

namespace SlugEnt.StreamProcessor
{
    public class StreamProducer : StreamBase
    {
        Producer _producer;


        public StreamProducer(string name) : base(name,EnumStreamType.Producer)
        {
        }



        public async Task PublishAsync()
        {
            _producer = await Producer.Create(
                new ProducerConfig(_streamSystem, _name)
                {
                    Reference = Guid.NewGuid().ToString(),


                    ConfirmationHandler = OnConfirmation
                });

            // Publish the messages
            for (var i = 0; i < 20; i++)
            {
                DateTime x = DateTime.Now;
                string time = x.ToShortTimeString();
                string msg = String.Format("Time: {0}   -->  Msg # {1}", time, i);
                var message = new Message(Encoding.UTF8.GetBytes(msg));
                await _producer.Send(message);
            }

        }
        public Task OnConfirmation (MessagesConfirmation confirmation)
        {
            switch (confirmation.Status)
            {
                // ConfirmationStatus.Confirmed: The message was successfully sent
                case ConfirmationStatus.Confirmed:
                    Console.WriteLine($"Message {confirmation.PublishingId} Publisher Confirmed!");
                    break;
                // There is an error during the sending of the message
                case ConfirmationStatus.WaitForConfirmation:
                case ConfirmationStatus.ClientTimeoutError
                    : // The client didn't receive the confirmation in time. 
                      // but it doesn't mean that the message was not sent
                      // maybe the broker needs more time to confirm the message
                      // see TimeoutMessageAfter in the ProducerConfig
                case ConfirmationStatus.StreamNotAvailable:
                case ConfirmationStatus.InternalError:
                case ConfirmationStatus.AccessRefused:
                case ConfirmationStatus.PreconditionFailed:
                case ConfirmationStatus.PublisherDoesNotExist:
                case ConfirmationStatus.UndefinedError:
                default:
                    Console.WriteLine(
                        $"Message  {confirmation.PublishingId} not confirmed. Error {confirmation.Status}");
                    break;
            }

            return Task.CompletedTask;
        }


    }
}
