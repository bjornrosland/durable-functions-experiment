using System;
using System.Collections.Generic;
using System.Text;

namespace DurableFunctions.Models
{
    public class BackupInfo
    {
        public int Files { get; set; }
        public long Bytes { get; set; }
        public string Phone { get; set; }
    }
}
