using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Data.Tables;
using DurableFunctions.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
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
            EntityId entityId = new EntityId(nameof(FilesCounter), context.InstanceId);
            while (!done)
            {
                string fileName = await context.WaitForExternalEvent<string>("BatchResponse", TimeSpan.FromMinutes(5));
                if (message.Files.Contains(fileName))
                {
                    log.LogInformation($"File {fileName} as been completed");
                    await context.CallEntityAsync(entityId, "Add", fileName);
                    int numCompletedTasks = await context.CallEntityAsync<int>(entityId, "Count");
                    log.LogInformation($"Instance {context.InstanceId} has completed {numCompletedTasks} tasks");
                    await context.CallActivityAsync("SingletonFunction", fileName);
                    done = numCompletedTasks == message.Files.Length;
                }
                else
                {
                    log.LogWarning($"The file '{fileName}' is not listed in this context");
                }
            }
            return done;
        }

        [FunctionName("SingletonFunction")]
        [Singleton(Mode = SingletonMode.Listener)]
        public static async Task SingletonFunctionAsync([ActivityTrigger]string fileName, ILogger log)
        {
            log.LogInformation($"Started singleton function with file name {fileName}");
            TimeSpan delay = TimeSpan.FromSeconds(30);
            await Task.Delay(delay);
            log.LogInformation($"Singleton function activated with file name {fileName}");
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
        public static async Task SetRowCompletedAsync([ActivityTrigger] (string instanceid, string fileName) input)
        {
            string querySting = $"InstanceId eq '{input.instanceid}' and FileName eq '{input.fileName}'";
            var rows = _tableClient.QueryAsync<TableEntity>(filter: querySting);
            await foreach (var row in rows)
            {
                row["Completed"] = true;
                await _tableClient.UpdateEntityAsync(row, row.ETag);
            }
        }
    }
}
