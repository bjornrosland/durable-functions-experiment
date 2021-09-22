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
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Http;

namespace LongRunningFunction
{
    public static class LongRunning
    {
        [FunctionName("LongRunningTask_Orchestrator")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var eventData = context.GetInput<Dictionary<string, string>>();
            string fileName = eventData.Values.First();
            await context.CallActivityAsync("LongRunningTask_Activity", fileName);
            return fileName;
        }

        [FunctionName("LongRunningTask_Activity")]
        [Singleton(Mode = SingletonMode.Function)]
        public static async Task LongRunningTaskActivityAsync([ActivityTrigger] string fileName, ILogger log)
        {
            TimeSpan delay = TimeSpan.FromSeconds(5);
            log.LogInformation($"Started creating object for {fileName}");
            await Task.Delay(delay);
            log.LogInformation($"Object for file {fileName} has been created");
        }

        [FunctionName("LongRunningTask_HttpStart")]
        public static async Task<DurableHttpResponse> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var eventData = await req.Content.ReadAsAsync<Dictionary<string, string>>();
            log.LogInformation($"Started with file name: {eventData}");
            string instanceId = await starter.StartNewAsync("LongRunningTask_Orchestrator", eventData);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            var response = GetDurableHttpResponse(instanceId, eventData);
            return response;
        }

        private static DurableHttpResponse GetDurableHttpResponse(string instanceId, Dictionary<string, string> eventData)
        {
            var content = new Dictionary<string, string>()
                    {
                        { "instanceId", instanceId },
                        { "eventData", JsonConvert.SerializeObject(eventData) }
                    };
            var headers = new HeaderDictionary
                    {
                        { "Content-Type", "application/json" }
                    };
            return new DurableHttpResponse(HttpStatusCode.Accepted, headers, JsonConvert.SerializeObject(content));
        }
    }
}