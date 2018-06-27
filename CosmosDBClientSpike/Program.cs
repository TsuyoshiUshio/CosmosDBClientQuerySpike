using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDBClientSpike
{

    public class DowntimeRecords
    {
        public string TeamId { get; set; }
        public DateTime Time { get; set; }

        public int Count { get; set; }
        public string id { get; set; }

    }
    class Program
    {

        private static DocumentClient client = null;
        private static string databaseId = null;
        static Program()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory());
            builder.AddJsonFile("appsettings.json");
            var config = builder.Build();
            client = new DocumentClient(new Uri(config["CosmosDBEndpointUri"]), config["CosmosDBPrimaryKey"]);
            databaseId = config["CosmosDBDatabaseId"];
        }

        public static async Task RemoveCollectionIfExists<T>()
        {
            try
            {
                await client.DeleteDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(databaseId, typeof(T).Name));
            }
            catch (DocumentClientException de)
            {
                if (de.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    // There is no document. Just ignore since this method is IfExists.
                    // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.documents.client.documentclient.deletedocumentcollectionasync?view=azure-dotnet
                }
                else
                {
                    throw de;
                }
            }
        }

        public static async Task CreateCollectionIfNotExists<T>(string partitionKey = "", int offerThroughput = 0)
        {
            var downtimeReportCollection = new DocumentCollection();
            downtimeReportCollection.Id = typeof(T).Name;
            if (!string.IsNullOrEmpty(partitionKey))
            {
                // Collection with PratitionKey
                downtimeReportCollection.PartitionKey.Paths.Add(partitionKey);
                await client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(databaseId),
                        downtimeReportCollection, new RequestOptions { OfferThroughput = offerThroughput });
            }
            else
            {
                // Collection without PartitionKey
                await client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri(databaseId),
                    downtimeReportCollection);
            }
        }

        private static async Task SeedAsync()
        {
            await RemoveCollectionIfExists<DowntimeRecords>();
            await CreateCollectionIfNotExists<DowntimeRecords>("/TeamId", 10000);

            for (int i = 0; i < 10000; i++)
            {
                var teamId = string.Format("Team{0:00}", i);
                var obj = new DowntimeRecords()
                {
                    TeamId = teamId,
                    Time = DateTime.Now,
                    id = teamId + "01",
                    Count = 2
                };
                await client.CreateDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(databaseId, typeof(DowntimeRecords).Name), obj,
                    new RequestOptions { PartitionKey = new PartitionKey(teamId) });
                obj.Count = 3;
                obj.id = teamId + "02";
                obj.Time = DateTime.Now;
                await client.CreateDocumentAsync(
                UriFactory.CreateDocumentCollectionUri(databaseId, typeof(DowntimeRecords).Name), obj,
                new RequestOptions { PartitionKey = new PartitionKey(teamId) });
            }
        }
        static void Main(string[] args)
        {

            // If you want to seed the data uncomment these lines.
            // Console.WriteLine("Seeding ...");
            // SeedAsync().GetAwaiter().GetResult();

            var teamId = "Team01";
            var sql = $"SELECT VALUE Sum(c.Count) from DowntimeRecords as c Where c.TeamId = \"{teamId}\"";

            var sw = new Stopwatch();
            sw.Start();
           var query = client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(databaseId, "DowntimeRecords"), sql);
           var result = query.ToList<dynamic>();
            sw.Stop();

           Console.WriteLine("Plain SQL query ------");
           Console.WriteLine(JsonConvert.SerializeObject(result));
           Console.WriteLine($"------- Elapse time (ms): {sw.ElapsedMilliseconds}");

            Console.WriteLine("LINQ Query ----- ");
            sw.Restart();
            var sum = client.CreateDocumentQuery<DowntimeRecords>(
                UriFactory.CreateDocumentCollectionUri(databaseId, "DowntimeRecords"))
                .Where<DowntimeRecords>(r => r.TeamId == teamId)
                .Sum<DowntimeRecords>(r => r.Count);
  
            Console.WriteLine($"Sum: {sum}");
            sw.Stop();
            Console.WriteLine($"------- Elapse time (ms): {sw.ElapsedMilliseconds}");

            Console.ReadLine();
        }
    }
}
