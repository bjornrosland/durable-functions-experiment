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
        private static readonly TableClient _tableClient = _tableServiceClient.GetTableClient("TestTable");

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
        public static async Task<bool> RunOrchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger log)
        {
            bool done = false;
            QueueMessageDto message = context.GetInput<QueueMessageDto>();
            string messageContent = JsonConvert.SerializeObject(message);
            log.LogInformation(messageContent);
            while (!done)
            {
                string fileName = await context.WaitForExternalEvent<string>("BatchResponse");
                if (message.Files.Contains(fileName))
                {
                    await context.CallActivityAsync("SetRowCompleted", (context.InstanceId, fileName));
                    log.LogInformation($"File {fileName} as been completed");
                    bool hasRunningTasks = await context.CallActivityAsync<bool>("HasRunningTasks", context.InstanceId);
                    done = !hasRunningTasks;
                }
                else
                {
                    log.LogWarning($"The file '{fileName}' is not listed in this context");
                }
            }
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

        private static async Task InsertRowsAsync(List<string> files, string instanceId)
        {
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
                Task insertTask = _tableClient.AddEntityAsync(row);
                insertTasks.Add(insertTask);
            });
            await Task.WhenAll(insertTasks);
        }

        [FunctionName("HasRunningTasks")]
        public static async Task<bool> HasRunningTasksAsync([ActivityTrigger] string instanceId)
        {
            string querySting = $"InstanceId eq '{instanceId}' and Completed eq false";
            var rows = _tableClient.QueryAsync<TableEntity>(filter: querySting);
            int numRows = 0;
            await foreach(var row in rows)
            {
                numRows++;
                break;
            }
            return numRows > 0;
        }

        [FunctionName("SetRowCompleted")]
        public static async Task SetRowCompletedAsync([ActivityTrigger] Tuple<string ,string> input)
        {
            string querySting = $"InstanceId eq '{input.Item1}' and FileName eq '{input.Item2}'";
            var rows = _tableClient.QueryAsync<TableEntity>(filter: querySting);
            await foreach (var row in rows)
            {
                row["Completed"] = true;
                await _tableClient.UpdateEntityAsync(row, row.ETag);
            }
        }
    }
}
