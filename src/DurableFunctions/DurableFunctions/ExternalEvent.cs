using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace DurableFunctions
{
    public static class ExternalEvent
    {
        [FunctionName("ExternalEvent")]
        public static async Task<bool> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            bool hasResponded = false;
            string eventName = "EventResponse";
            TimeSpan timeOut = TimeSpan.FromMinutes(1);
            log.LogInformation("Started orchestrator");

            try
            {
                await context.WaitForExternalEvent(eventName, timeOut);
                hasResponded = true;
            }
            catch(TimeoutException)
            {
                log.LogWarning("Time out!");
            }

            if (hasResponded)
                log.LogInformation("The user has responded in time");

            return hasResponded;
        }

        [FunctionName("ExternalEvent_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId = await starter.StartNewAsync("ExternalEvent", null);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            var statusReponse = starter.CreateCheckStatusResponse(req, instanceId);
            return statusReponse;
        }
    }
}