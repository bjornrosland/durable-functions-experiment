using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Azure.Data.Tables;
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
        private static readonly TableServiceClient _tableServiceClient = new TableServiceClient(Environment.GetEnvironmentVariable("BlobTable"));

        [FunctionName("BlobQueue")]
        public static async Task RunAsync([QueueTrigger("test-queue", Connection = "BlobQueue")]string queueMessage,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var message = JsonConvert.DeserializeObject<QueueMessageDto>(queueMessage);
            string instanceId = await starter.StartNewAsync("BatchOrchestrator", message);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            await InsertRowsAsync(message.Files.ToList(), instanceId);
            log.LogInformation("Inserted rows to table");
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

        private static async Task InsertRowsAsync(List<string> files, string instanceId, string tableName="TestTable")
        {
            var tableClient = _tableServiceClient.GetTableClient(tableName);
            string partitonKey = Guid.NewGuid().ToString();
            List<Task> insertTasks = new List<Task>();
            files.ForEach(file =>
            {
                string rowKey = Guid.NewGuid().ToString();
                TableEntity row = new TableEntity(partitonKey, rowKey)
                {
                    {"InstanceId" , instanceId},
                    {"FileName", file },
                    {"Completed", false }
                };
                Task insertTask = tableClient.AddEntityAsync(row);
                insertTasks.Add(insertTask);
            });
            await Task.WhenAll(insertTasks);
        }
    }
}
