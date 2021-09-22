using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Data.Tables;
using DurableFunctions.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

namespace DurableFunctions
{
    public static class BlobQueue
    {
        private static readonly TableServiceClient _tableServiceClient = new TableServiceClient(Environment.GetEnvironmentVariable("BlobTable"));
        private static readonly TableClient _tableClient = _tableServiceClient.GetTableClient("TestTable");

        [FunctionName("BlobQueue")]
        public static async Task RunAsync([QueueTrigger("test-queue", Connection = "BlobQueue")] QueueMessageDto message,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId = await starter.StartNewAsync("BatchOrchestrator", message);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            string responseUri = await GetResponseUriAsync(starter, instanceId);
            log.LogInformation(responseUri);
        }

        [FunctionName("BatchOrchestrator")]
        public static async Task<bool> RunOrchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger log)
        {
            bool done = false;
            QueueMessageDto message = context.GetInput<QueueMessageDto>();
            EntityId entityId = new EntityId(nameof(FilesDurableEntity), context.InstanceId);
            TimeSpan timeout = TimeSpan.FromMinutes(5);
            while (!done)
            {
                string fileName = await context.WaitForExternalEvent<string>("BatchResponse", timeout);
                log.LogInformation($"Received file {fileName}");
                if (message.Files.Contains(fileName))
                {
                    bool isAlreadyAdded = await context.CallEntityAsync<bool>(entityId, "Contains", fileName);
                    if (isAlreadyAdded)
                    {
                        log.LogWarning($"The file {fileName} has alrady been added. Skipping file.");
                        continue;
                    }
                    context.SignalEntity(entityId, "Add", fileName);
                    var durableRequest = GetLongRunningTaskRequest(fileName);
                    var durableResonse = await context.CallHttpAsync(durableRequest);
                    log.LogInformation($"Started creating long running task with file name: {fileName}");
                    int completedFilesCount = await context.CallEntityAsync<int>(entityId, "Count");
                    log.LogInformation($"Number of completed files: {completedFilesCount}");
                    done = message.Files.Length == completedFilesCount;
                }
                else
                {
                    log.LogWarning($"The file '{fileName}' is not listed in this context");
                }
            }
            context.SignalEntity(entityId, "Delete");
            return done;
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

        private static Uri GetFunctionUri(string functionName)
        {
#if DEBUG
            UriBuilder builder = new UriBuilder
            {
                Scheme = "http",
                Host = "localhost",
                Port = 7072,
                Path = $"api/{functionName}"
            };
#else
            UriBuilder builder = new UriBuilder
            {
                Scheme = "https",
                Host = Environment.GetEnvironmentVariable("WEBSITE_HOSTNAME"),
                Path = $"api/{functionName}"
            };
#endif
            return builder.Uri;
        }

        private static DurableHttpRequest GetLongRunningTaskRequest(string fileName)
        {
            Uri functionuUri = GetFunctionUri("LongRunningTask_HttpStart");
            var content = new Dictionary<string, string>()
                    {
                        { "fileName", fileName }
                    };
            var header = new HeaderDictionary
                    {
                        { "Content-Type", "application/json" }
                    };
            var durableRequest = new DurableHttpRequest(method: HttpMethod.Post,
                uri: functionuUri,
                content: JsonConvert.SerializeObject(content),
                headers: header);
            return durableRequest;
            
        }
    }
}
