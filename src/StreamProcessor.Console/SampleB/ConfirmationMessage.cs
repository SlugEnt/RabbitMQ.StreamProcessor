using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;

namespace StreamProcessor.ConsoleScr.SampleB
{
    public class ConfirmationMessage
    {
        public ConfirmationMessage(Message message, bool successful, string batchId)
        {
            Message = message;
            Successful = successful;
            BatchId = batchId;
        }


        public Message Message { get; set; }
        public bool Successful { get; set; }

        public string BatchId { get; set; }
    }
}
