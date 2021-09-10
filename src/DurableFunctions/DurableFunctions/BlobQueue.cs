using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace DurableFunctions
{
    public static class BlobQueue
    {
        [FunctionName("BlobQueue")]
        public static void Run([QueueTrigger("test-queue", Connection = "BlobQueue")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
        }
    }
}
