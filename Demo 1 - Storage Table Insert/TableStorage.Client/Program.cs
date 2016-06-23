using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Net;
using System.Threading;

namespace TableStorage.Client
{
    class Program
    {
        static void Main(string[] args)
        {
            const string tableName = "orders";
            const int numEntities = 200;

            // Setup default connection limit to support parallel uploads
            ServicePointManager.DefaultConnectionLimit = 30;
            ThreadPool.SetMinThreads(30, 30);

            // Turn Nagle Off
            //ServicePointManager.UseNagleAlgorithm = false;

            // Connect to the storage account
            CloudStorageAccount storageAccount = CreateStorageAccountFromConnectionString(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create a table client for interacting with the table service
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Get a refernce to the table.
            CloudTable table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();

            TimeSpan timeSpan;
            Console.WriteLine(string.Format("Inserting {0} entities without any performance optimizations.", numEntities));
            timeSpan = InsertEntities(CreateEntities(numEntities), table);
            Console.WriteLine(string.Format("Finished in {0} seconds.\n", timeSpan.TotalSeconds));

            Console.WriteLine(string.Format("Inserting {0} entities in parallel.", numEntities));
            timeSpan = InsertEntitiesInParallel(CreateEntities(numEntities), table);
            Console.WriteLine(string.Format("Finished in {0} seconds.\n", timeSpan.TotalSeconds));

            Console.WriteLine(string.Format("Inserting {0} entities in batch.", numEntities));
            timeSpan = InsertEntitiesInBatch(CreateEntities(numEntities), table);
            Console.WriteLine(string.Format("Finished in {0} seconds.\n", timeSpan.TotalSeconds));

            Console.WriteLine("Press any key to continue ...");
            Console.ReadLine();
        }

        private static TimeSpan InsertEntities(List<DynamicTableEntity> entities, CloudTable table)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            foreach (DynamicTableEntity entity in entities)
            {
                TableOperation tableInsertEntity = TableOperation.Insert(entity);
                table.Execute(tableInsertEntity);
            }

            sw.Stop();
            return sw.Elapsed;
        }

        private static TimeSpan InsertEntitiesInParallel(List<DynamicTableEntity> entities, CloudTable table)
        {
            ParallelOptions options = new ParallelOptions();
            options.MaxDegreeOfParallelism = 10;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            Parallel.ForEach(entities, options, entity =>
            {
                TableOperation tableInsertEntity = TableOperation.Insert(entity);
                table.Execute(tableInsertEntity);
            });

            sw.Stop();
            return sw.Elapsed;
        }

        private static TimeSpan InsertEntitiesInBatch(List<DynamicTableEntity> entities, CloudTable table)
        {
            TableBatchOperation batchOp = new TableBatchOperation();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            foreach (DynamicTableEntity entity in entities)
            {
                batchOp.Add(TableOperation.Insert(entity));
                if (batchOp.Count > 99)
                {
                    table.ExecuteBatch(batchOp);
                    batchOp.Clear();
                }
            }

            if (batchOp.Count > 0)
            {
                table.ExecuteBatch(batchOp);
            }

            sw.Stop();
            return sw.Elapsed;
        }

        private static List<DynamicTableEntity> CreateEntities(int numEntities)
        {
            List<DynamicTableEntity> entities = new List<DynamicTableEntity>();
            for (int index = 0; index < numEntities; index++)
            {
                entities.Add(new DynamicTableEntity(
                    "ordernums", Guid.NewGuid().ToString().Replace("-","")));
            }

            return entities;
        } 

        private static CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString)
        {
            
            CloudStorageAccount storageAccount;
            try
            {
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (FormatException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the application.");
                throw;
            }
            catch (ArgumentException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return storageAccount;
        }
    }
}
