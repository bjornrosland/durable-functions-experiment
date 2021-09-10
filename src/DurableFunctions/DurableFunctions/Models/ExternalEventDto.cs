using System;
using System.Collections.Generic;
using System.Text;

namespace DurableFunctions.Models
{
    public class ExternalEventDto
    {
        public string Id { get; set; }
        public string StatusQueryGetUri { get; set; }
        public string SendEventPostUri { get; set; }
        public string TerminatePostUri { get; set; }
        public string PurgeHistoryDeleteUri { get; set; }
    }
}
