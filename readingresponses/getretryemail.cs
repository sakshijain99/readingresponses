using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using Newtonsoft.Json;
using System.Globalization;
using Newtonsoft.Json.Converters;
using System.Net.Mail;
using System.IO;
using OfficeOpenXml;
using Microsoft.Azure.Cosmos;
using System.Net;
using Microsoft.Azure.Cosmos.Linq;
using Renci.SshNet;

namespace cosmosdbquery
{
   
public class getretryemail
    {
        public string statusCode { get; set; }
        public string reasonCode { get; set; }
        [JsonProperty(PropertyName = "id")]
    
        public string id { get; set; }
        public string partitionKey { get; set; }
        public object hashKey { get; set; }
        public DateTime timestamp { get; set; }
        public DateTime processedtimestamp { get; set; }
        public int counter = 0;// { get; set; }
    }
    public class getmainmail
    {
        //[JsonProperty(PropertyName = "id")]
       // public string Id { get; set; }

        public VerifyEmailResponse VerifyEmailResponse { get; set; }
        public string id { get; set; }
      
        public string partitionKey { get; set; }
    }
    public  class VerifyEmailResponse
    {
        public VerifyEmailResult VerifyEmailResult { get; set; }
    }
    public  class VerifyEmailResult
    {
        public ServiceStatus ServiceStatus { get; set; }
        public ServiceResult ServiceResult { get; set; }
    }

    public class ServiceResult
    {
        public DateTimeOffset Timestamp { get; set; }
        public Email Email { get; set; }
        public Reason Reason { get; set; }
        public Hygiene Hygiene { get; set; }
        public SendRecommendation SendRecommendation { get; set; }
        public DestinationCountry DestinationCountry { get; set; }
        public bool Cached { get; set; }
        public bool Disposable { get; set; }
        public bool PotentiallyVulgar { get; set; }
        public bool RoleBased { get; set; }
        public string EmailSegment { get; set; }
    }
    public class DestinationCountry
    {
        public long Code { get; set; }
        public string Alpha2Code { get; set; }
        public string Alpha3Code { get; set; }
        public string Name { get; set; }
    }
    public class Email
    {
        
        public string Complete { get; set; }
       
        public string LocalPart { get; set; }
        public string DomainPart { get; set; }
    }
    public class Hygiene
    {
        public string HygieneResult { get; set; }
        public bool NetProtected { get; set; }
        public object NetProtectedBy { get; set; }
    }
    public  class Reason
    {
        public long Code { get; set; }
        public string Description { get; set; }
    }
    public class SendRecommendation
    {
        public string Recommendation { get; set; }
        public long RecommendedRetries { get; set; }
        public long RecommendedRetryDelaySeconds { get; set; }
    }

    public  class ServiceStatus
    {
        public long StatusNbr { get; set; }
        public string StatusDescription { get; set; }
    }


    public class process {

        public static readonly string EndpointUri = "https://learn-cos.documents.azure.com:443/";

        public static readonly string PrimaryKey = "PS7eKnpOSJ2aInEGeJbiqk4V8Vab7vZx9OFYUUajjDlhLWpnsgmwuASX0sJ6UAibjy9YYqJDWLGiexqyYJ2uaQ==";
        public CosmosClient cosmosClient;
        public Database database;
        public Container containermain;
        public Container containerretry;
        List<getmainmail> listmain = new List<getmainmail>();
        List<getmainmail> mainquery = new List<getmainmail>();
        List<getretryemail> listretry = new List<getretryemail>();
        List<getretryemail> retrysearchlist = new List<getretryemail>();
        public async Task GetReference()
        {
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions()
            {
              ConnectionMode = ConnectionMode.Gateway
           });
            database = cosmosClient.GetDatabase("mydb");
          containermain = database.GetContainer("collection1");
            containerretry = database.GetContainer("mycollection");
        }
        
        public async Task<bool> ItemExistsInMainCollection(string id)
        {
            var sqlQueryText = $"SELECT * FROM c WHERE c.id =\"{id}\" ";
          //  Console.WriteLine(sqlQueryText);
           // Console.ReadKey();
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<getmainmail> queryResultSetIterator = this.containermain.GetItemQueryIterator<getmainmail>(queryDefinition);
            FeedResponse<getmainmail> currentResultSet = await queryResultSetIterator.ReadNextAsync();
            if (currentResultSet.Count != 0)
                return true;
            else
                return false;
        
             }
        public async Task QueryItemsRetryAsync()
        {
            var currentdatetime = DateTime.UtcNow;
            var updatedtime = currentdatetime.AddHours(-26);

            var sqlQueryText = $"SELECT * FROM c WHERE ((c.processedtimestamp<\"{updatedtime.ToString("yyyy-MM-ddTHH\\:mm\\:ss.ffffffZ")}\" " + "OR NOT IS_DEFINED(c.processedtimestamp))"+ "AND (c.counter<=3 OR NOT IS_DEFINED(c.counter)))";
           // Console.WriteLine(sqlQueryText);
            //Console.ReadKey();
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<getretryemail> queryResultSetIterator = this.containerretry.GetItemQueryIterator<getretryemail>(queryDefinition);
         
            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<getretryemail> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (getretryemail item in currentResultSet)
                {
                    
                    listretry.Add(item);
                    if (listretry.Count == 10)
                        break;
              
                }
            }
           // Console.WriteLine(listretry.Count);
        }
        public  async Task AddItemstoMainContainer(getmainmail obj)
        {
            try
            {
                ItemResponse<getmainmail> Response = await containermain.UpsertItemAsync<getmainmail>(obj,new PartitionKey(obj.id.Substring(0,2)));
                Console.WriteLine("updated item in database with id: {0} \n", Response.Resource.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete);
                            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
                    }
        public async Task UpdateItemstoRetryContainer(getretryemail obj)
        {
            try
            {
                ItemResponse<getretryemail> Response = await containerretry.UpsertItemAsync<getretryemail>(obj);
                Console.WriteLine("updated item in database with id: {0} \n", Response.Resource.id);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public async Task searchRetry(string id)
        {
            retrysearchlist.Clear();
            var sqlQueryText = $"SELECT * FROM c WHERE c.id =\"{id}\" ";
            //  Console.WriteLine(sqlQueryText);
            // Console.ReadKey();
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<getretryemail> queryResultSetIterator = this.containerretry.GetItemQueryIterator<getretryemail>(queryDefinition);
            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<getretryemail> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (getretryemail item in currentResultSet)
                {
                    retrysearchlist.Add(item);

                }
            }

        }
        public async Task DeleteItem(string id,PartitionKey p)
        {
            try
            {
                ItemResponse<getretryemail> Response = await containerretry.DeleteItemAsync<getretryemail>(id, p);
                Console.WriteLine("Deleted item {0}\n", id);
               // Console.ReadKey();

            }
            catch (Exception e)
            {
                Console.WriteLine("item not found\n");
            }
        }
        public async Task AddingAndDeleting(getmainmail obj)
        {

            await AddItemstoMainContainer(obj);
            await searchRetry(obj.id);
            if (retrysearchlist.Count != 0)
            {
                var item = retrysearchlist[0];
                await DeleteItem(item.id, new PartitionKey(item.reasonCode));
            }
            else { Console.WriteLine("item not found"); }
        }

        public async Task Readfromdb() {
           
           
            await GetReference();
        
            await QueryItemsRetryAsync();
           
            List<getretryemail> retrylist = new List<getretryemail>();
          
            foreach (var mail in listretry.ToList())
            {
                mail.counter++;
           
                if (await ItemExistsInMainCollection(mail.id))

                    {
                        Console.WriteLine("item found");
                        await DeleteItem(mail.id, new PartitionKey(mail.reasonCode));
                    }
                    else
                    {
                        Console.WriteLine("item not found");
                        retrylist.Add(mail);
                        mail.processedtimestamp = DateTime.UtcNow;

                        await UpdateItemstoRetryContainer(mail);
                    }
            }
                   
            WriteExcelDocument(retrylist);
            SendFileToInformatica();
        }

        void SendFileToInformatica()
        {
            string filepath = @"C:\Users\Lenovo\Documents\emailvalidation.xlsx";
            using (SftpClient sftpClient = new SftpClient("sftp.strikeiron.com", 22, "microsofttest", "Ch@!jYmW"))
            {
                Console.WriteLine("Connect to server");
                sftpClient.Connect();
                sftpClient.ChangeDirectory(@"/To_Strikeiron");
                Console.WriteLine("Creating FileStream object to stream a file");
                using (FileStream fs = new FileStream(filepath, FileMode.Open))
                {
                    sftpClient.BufferSize = 1024;
                    sftpClient.UploadFile(fs, Path.GetFileName(filepath));
                }
                Console.WriteLine("file sent to informatica");
                Console.ReadKey();
                sftpClient.Dispose();
            }
        }

        void WriteExcelDocument(List<getretryemail> re)
        {

            using (ExcelPackage excel = new ExcelPackage())
            {
                excel.Workbook.Worksheets.Add("Worksheet1");
                
                string headerRange = "A0";

                // Target a worksheet
                var worksheet = excel.Workbook.Worksheets["Worksheet1"];
                foreach(getretryemail item in re)
                {
                    string number = headerRange.Substring(1);
                 
                    headerRange = headerRange.Remove(1);
                    int i = int.Parse(number) + 1;
                    headerRange += i.ToString();
               
                    worksheet.Cells[headerRange].Value = item.id;
                }
                FileInfo excelFile = new FileInfo(@"C:\Users\Lenovo\Documents\emailvalidation.xlsx");
                excel.SaveAs(excelFile);
                Console.WriteLine("file written");
            }
        }
    }

    }

   