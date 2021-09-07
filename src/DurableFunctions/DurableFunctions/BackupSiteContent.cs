using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace DurableFunctions
{
    public static class BackupSiteContent
    {
        [FunctionName("E2_BackupSiteContent")]
        public static async Task<long> Run(
            [OrchestrationTrigger] IDurableOrchestrationContext backupContext)
        {
            string rootDirectory = backupContext.GetInput<string>()?.Trim();
            if (string.IsNullOrEmpty(rootDirectory))
            {
                rootDirectory = Directory.GetParent(typeof(BackupSiteContent).Assembly.Location).FullName;
            }

            string[] files = await backupContext.CallActivityAsync<string[]>(
                "E2_GetFileList",
                rootDirectory);

            var tasks = new Task<long>[files.Length];
            for (int i = 0; i < files.Length; i++)
            {
                tasks[i] = backupContext.CallActivityAsync<long>(
                    "E2_CopyFileToBlob",
                    files[i]);
            }

            await Task.WhenAll(tasks);

            long totalBytes = tasks.Sum(t => t.Result);
            return totalBytes;
        }

        [FunctionName("E2_GetFileList")]
        public static string[] GetFileList(
            [ActivityTrigger] string rootDirectory,
            ILogger log)
        {
            log.LogInformation($"Searching for files under '{rootDirectory}'...");
            string[] files = Directory.GetFiles(rootDirectory, "*", SearchOption.AllDirectories);
            log.LogInformation($"Found {files.Length} file(s) under {rootDirectory}.");

            return files;
        }

        [FunctionName("E2_CopyFileToBlob")]
        public static async Task<long> CopyFileToBlob(
            [ActivityTrigger] string filePath,
            Binder binder,
            ILogger log)
        {
            long byteCount = new FileInfo(filePath).Length;

            // strip the drive letter prefix and convert to forward slashes
            string blobPath = filePath
                .Substring(Path.GetPathRoot(filePath).Length)
                .Replace('\\', '/');
            string outputLocation = $"backups/{blobPath}";

            log.LogInformation($"Copying '{filePath}' to '{outputLocation}'. Total bytes = {byteCount}.");

            // copy the file contents into a blob
            using (Stream source = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (Stream destination = await binder.BindAsync<CloudBlobStream>(
                new BlobAttribute(outputLocation, FileAccess.Write)))
            {
                await source.CopyToAsync(destination);
            }

            return byteCount;
        }

        [FunctionName("E2_BackupSiteContent_Init")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("E2_BackupSiteContent", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}