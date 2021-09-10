using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using DurableFunctions.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DurableFunctions
{
    public static class BlobQueue
    {
        [FunctionName("BlobQueue")]
        public static async Task RunAsync([QueueTrigger("test-queue", Connection = "BlobQueue")]string queueMessage,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var message = JsonConvert.DeserializeObject<QueueMessageDto>(queueMessage);
            string instanceId = await starter.StartNewAsync("BatchOrchestrator", message);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            string responseUri = await GetResponseUriAsync(starter, instanceId);
            log.LogInformation(responseUri);
        }

        [FunctionName("BatchOrchestrator")]
        public static async Task<List<string>> RunOrchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger log)
        {
            bool done = false;
            List<string> files = new List<string>();
            QueueMessageDto message = context.GetInput<QueueMessageDto>();
            string messageContent = JsonConvert.SerializeObject(message);
            log.LogInformation(messageContent);
            while (!done)
            {
                string fileName = await context.WaitForExternalEvent<string>("BatchResponse");
                if (message.Files.Contains(fileName))
                {
                    log.LogInformation($"Completed with file {fileName}");
                    files.Add(fileName);
                    done = true;
                }
            }
            return files;
        }

        private static async Task<string> GetResponseUriAsync(IDurableOrchestrationClient starter,
            string instanceId,
            string eventName="BatchResponse")
        {
            HttpRequestMessage req = new HttpRequestMessage();
            var statusReponse = starter.CreateCheckStatusResponse(req, instanceId);
            string content = await statusReponse.Content.ReadAsStringAsync();
            ExternalEventDto dto = JsonConvert.DeserializeObject<ExternalEventDto>(content);
            string responseUri = dto.SendEventPostUri.Replace("{eventName}", eventName);
            return responseUri;
        }
    }
}
