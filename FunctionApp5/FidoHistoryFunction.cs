using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Microsoft.Azure; 
using Microsoft.WindowsAzure.Storage; 
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace FidoHistory
{
    public static class FidoHistoryFunction
    {
        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("testtopic", "mysubscription", AccessRights.Manage, Connection = "conn")]string msg, TraceWriter log)
        {
            log.Info($"C# ServiceBus topic trigger function processed message: {msg}");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                    CloudConfigurationManager.GetSetting("storageacct"));

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            CloudTable table = tableClient.GetTableReference("REQUESTS");
            log.Info("NAME-> "+table.Name);
            // Deserialization from JSON  
            

            RequestEntity obj = JsonConvert.DeserializeObject<RequestEntity>(msg);
            log.Info("ParitionKey->"+obj.PartitionKey);
            obj.json = msg;

            TableOperation insertOperation = TableOperation.Insert(obj);

            // Execute the insert operation.
            table.Execute(insertOperation);

        }
    }
}



public class RequestEntity : TableEntity
{
    private string cid;
    private string sid;
    private string payload;

    public string correlationid
    {
        get { return cid; }
        set {
            cid = value;
            this.RowKey = value;
        }
    }
    public string storeid
    { 
        get { return sid; }
        set {
            sid = value;
            this.PartitionKey = value;
        }
    }

    public string json
    {
        get { return payload;  }
        set { payload = value;  }
    }
}
