using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableFunctions.Models
{
    [JsonObject(MemberSerialization.OptIn)]
    public class FilesDurableEntity
    {
        [JsonProperty("files")]
        public List<string> Files { get; set; } = new List<string>();


        public Task Add(string fileName)
        {
            lock (Files)
            {
                if(!Files.Contains(fileName))
                    Files.Add(fileName);
            }
            return Task.CompletedTask;
        }

        public Task<bool> Contains(string filename)
        {
            bool hasFile = Files.Contains(filename);
            return Task.FromResult(hasFile);
        }

        public Task<bool> SequenceEqual(List<string> files)
        {
            bool areEqual = Files.SequenceEqual(files);
            return Task.FromResult(areEqual);
        }

        public Task Reset()
        {
            Files = new List<string>();
            return Task.CompletedTask;
        }

        public Task<List<string>> Get()
        {
            return Task.FromResult(Files);
        }

        public Task<int> Count()
        {
            return Task.FromResult(Files.Count());
        }

        public void Delete()
        {
            Entity.Current.DeleteState();
        }

        [FunctionName(nameof(FilesDurableEntity))]
        public static Task Run([EntityTrigger] IDurableEntityContext ctx)
            => ctx.DispatchAsync<FilesDurableEntity>();
    }
}
