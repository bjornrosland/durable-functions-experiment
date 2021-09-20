using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace LongRunningFunction
{
    public static class LongRunning
    {
        [FunctionName("LongRunningTask_Orchestrator")]
        public static async Task<bool> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var eventData = context.GetInput<Dictionary<string, string>>();
            bool isCompleted = await context.CallActivityAsync<bool>("LongRunningTask_Activity", eventData);
            return isCompleted;
        }

        [FunctionName("LongRunningTask_Activity")]
        [Singleton(Mode = SingletonMode.Function)]
        public static async Task<bool> LongRunningTaskAsync([ActivityTrigger] Dictionary<string, string> eventData, ILogger log)
        {
            string fileName = eventData.Values.First();
            TimeSpan delay = TimeSpan.FromSeconds(15);
            log.LogInformation($"Started creating object for {fileName}");
            await Task.Delay(delay);
            log.LogInformation($"Object for file {fileName} has been created");
            return true;
        }

        [FunctionName("LongRunningTask_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var eventData = await req.Content.ReadAsAsync<Dictionary<string, string>>();
            log.LogInformation($"Started with file name: {eventData}");
            string instanceId = await starter.StartNewAsync("LongRunningTask_Orchestrator", eventData);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}