using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using DurableFunctions.Models;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace DurableFunctions
{
    public static class ExternalEvent
    {
        [FunctionName("ExternalEvent")]
        public static async Task<bool> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            int id = context.GetInput<int>();
            bool hasResponded = false;
            string eventName = $"EventResponse_{id}";
            TimeSpan timeOut = TimeSpan.FromMinutes(2);
            log.LogInformation($"Started orchestration of ID: {id}");

            try
            {
                await context.WaitForExternalEvent(eventName, timeOut);
                hasResponded = true;
            }
            catch (TimeoutException)
            {
                log.LogWarning($"Time out for ID {id}");
            }

            if (hasResponded)
                log.LogInformation($"The ID {id} responded in time");

            return hasResponded;
        }

        [FunctionName("GetIds")]
        public static int[] GetIds(
            [ActivityTrigger] int numberOfIds,
            ILogger log)
        {
            log.LogInformation($"Number of IDs: {numberOfIds}");
            var rand = new Random(Guid.NewGuid().GetHashCode());
            int[] ids = new int[numberOfIds];
            for(int i = 0; i < numberOfIds; i++)
            {
                int id = rand.Next(10000);
                log.LogInformation($"Created ID: {id}");
                ids[i] = id;
            }
            return ids;

        }

        [FunctionName("ExternalEvent_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId = await starter.StartNewAsync("OrchestrateFiles", null);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            var statusReponse = starter.CreateCheckStatusResponse(req, instanceId);
            //var content = await statusReponse.Content.ReadAsStringAsync();
            //var dto = JsonConvert.DeserializeObject<ExternalEventDto>(content);
            //string sendEventPostUri = dto.SendEventPostUri.Replace("{eventName}", "ExternalEvent");
            //log.LogInformation(sendEventPostUri);
            return statusReponse;
        }

        [FunctionName("OrchestrateFiles")]
        public static async Task OrchestrateFiles(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            int[] ids = await context.CallActivityAsync<int[]>("GetIds", 2);

            // Run multiple device provisioning flows in parallel
            var provisioningTasks = new List<Task>();
            foreach (int id in ids)
            {
                Task task = context.CallSubOrchestratorAsync("ExternalEvent", id);
                provisioningTasks.Add(task);
            }

            await Task.WhenAll(provisioningTasks);

        }

    }
}