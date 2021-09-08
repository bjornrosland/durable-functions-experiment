using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Twilio.Rest.Api.V2010.Account;
using Twilio.Types;

namespace DurableFunctions
{
    public static class BackupSiteContent
    {
        [FunctionName("E2_BackupSiteContent")]
        public static async Task<BackupInfo> Run(
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

            BackupInfo results = new BackupInfo()
            {
                Files = tasks.Length,
                Bytes = tasks.Sum(t => t.Result),
                Phone = "+4799247917"
            };

            await backupContext.CallActivityAsync("SMS_Notififaction", results);
            
            return results;
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
                try
                {
                    log.LogInformation($"Backup of file '{filePath}'");
                    await source.CopyToAsync(destination);
                }
                catch(Exception e)
                {
                    log.LogError($"Could not upload file {source} to {destination}.\nReason: {e.Message}");
                }
            }

            return byteCount;
        }

        [FunctionName("SMS_Notififaction")]
        public static void SendSmsChallenge(
            [ActivityTrigger] BackupInfo backupInfo,
            ILogger log,
            [TwilioSms(AccountSidSetting = "TwilioAccountSid", AuthTokenSetting = "TwilioAuthToken", From = "%TwilioPhoneNumber%")]
        out CreateMessageOptions message)
        {

            log.LogInformation($"Sending backup info to {backupInfo.Phone}.");
            double megaBytes = backupInfo.Bytes / Math.Pow(1024, 2);

            message = new CreateMessageOptions(new PhoneNumber(backupInfo.Phone))
            {
                Body = $"Your files have been backed up to your system.\nFiles: {backupInfo.Files}\nSize: {megaBytes:F2} MB"
            };

        }

        [FunctionName("E2_BackupSiteContent_Init")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string instanceId = await starter.StartNewAsync("E2_BackupSiteContent", null);
            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");
            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}