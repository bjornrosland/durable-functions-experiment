using System;
using System.Collections.Generic;
using System.Text;

namespace DurableFunctions.Models
{
    public class QueueMessageDto
    {
        public string JobId { get; set; }
        public string[] Files { get; set; }
    }
}
