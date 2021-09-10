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
using System.Linq;

namespace DurableFunctions
{
    
    public static class ExternalEvent
    {
        private static readonly List<int> _ids = new List<int> { 10, 20 };
        private static readonly List<int> _idsFromEvent = new List<int>();

        [FunctionName("ExternalEvent")]
        public static async Task<bool> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            bool hasAllIds = false;
            string eventName = "ResponseId";
            log.LogInformation($"Event name: {eventName}");
            while (!hasAllIds)
            {
                int idFromEvent = await context.WaitForExternalEvent<int>(eventName);
                if (_ids.Contains(idFromEvent))
                {
                    log.LogInformation("Correct ID");
                    lock (_idsFromEvent)
                    {
                        _idsFromEvent.Add(idFromEvent);
                    }
                }
                else
                {
                    log.LogInformation($"The ID {idFromEvent} is not in list");
                }

                if (_ids.SequenceEqual(_idsFromEvent.Distinct()))
                {
                    log.LogInformation("We have all the IDs");
                    hasAllIds = true;
                }
                else
                {
                    log.LogInformation("Still running");
                }


            }
            return hasAllIds;

        }

        [FunctionName("ExternalEvent_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            _idsFromEvent.Clear();
            string instanceId = await starter.StartNewAsync("ExternalEvent", null);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            var statusReponse = starter.CreateCheckStatusResponse(req, instanceId);
            return statusReponse;
        }

    }
}