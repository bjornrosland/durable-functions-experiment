using System;
using System.Net.Http;
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
        public static async Task RunAsync([QueueTrigger("test-queue", Connection = "BlobQueue")]string myQueueItem,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var message = JsonConvert.DeserializeObject<QueueMessageDto>(myQueueItem);
            string instanceId = await starter.StartNewAsync("BatchOrchestrator", message);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            string responseUri = await GetResponseUriAsync(starter, instanceId);
            log.LogInformation(responseUri);
        }

        [FunctionName("BatchOrchestrator")]
        public static async Task RunOrchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger log)
        {
            QueueMessageDto message = context.GetInput<QueueMessageDto>();
            string messageContent = JsonConvert.SerializeObject(message);
            log.LogInformation(messageContent);
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
