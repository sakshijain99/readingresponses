using cosmosdbquery;
using Microsoft.Azure.Cosmos;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Services.AppAuthentication;

namespace readingresponses
{
    public class Key
    {
        public string name = "EmailId";
        public string datatype = "STRING";
        public string value { get; set; }
    }

    public class Attribute
    {
        public string key { get; set; }
        public string datatype = "STRING";
        public string value { get; set; }
    }

    public class signal
    {
        public string impactedEntity = "Contact";
        public string signalType = "EmailValidationStatusChange";
        public string action = "Update";
        public List<Key> keys { get; set; }
        public List<Attribute> attributes { get; set; }
        public object headers { get; set; }
        public string correlationId { get; set; }
        public string originatingSystem = "Informatica";
        public DateTime originatingSystemDate { get; set; }
        public string internalProcessor = "CVES";
        public DateTime internalProcessingDate { get; set; }
        public DateTime signalAcquisitionDate { get; set; }
    }
    class Program
    {
        public static readonly string EndpointUri = "https://learn-cos.documents.azure.com:443/";

        public static readonly string PrimaryKey = "PS7eKnpOSJ2aInEGeJbiqk4V8Vab7vZx9OFYUUajjDlhLWpnsgmwuASX0sJ6UAibjy9YYqJDWLGiexqyYJ2uaQ==";
        public CosmosClient cosmosClient;
        public Database database;
        public Container containermain;
        public Container containerretry;
        public async Task GetReferencing()
        {
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions()
            {
                ConnectionMode = ConnectionMode.Gateway
            });
            database = cosmosClient.GetDatabase("mydb");
            containermain = database.GetContainer("collection1");
            containerretry = database.GetContainer("mycollection");
        }
        static async Task Main(string[] args)
        {
            string strFilePath = @"C:\Users\Lenovo\Documents\emailvalidation_Processed.csv";
            DateTime lastModified = System.IO.File.GetLastWriteTime(strFilePath).ToUniversalTime();
            DownloadFileFromInformatica();
            process p = new process();
            Program pobject = new Program();
            await pobject.GetReferencing();
            p.cosmosClient = pobject.cosmosClient;
            p.database = pobject.database;
            p.containermain = pobject.containermain;
            p.containerretry = pobject.containerretry;
            List<getmainmail> responses = File.ReadAllLines(@"C:\Users\Lenovo\Documents\emailvalidation_Processed.csv").Skip(1).Select(v => FromCsv(v)).ToList();
            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("200", "EMAIL_VALID");
            dict.Add("210", "DOMAIN_EXISTS");
            dict.Add("220", "RETRY");
            dict.Add("250", "EMAIL_EXISTS_BUT_SPAM");
            dict.Add("260", "DOMAIN_EXISTS_BUT_SPAM");
            dict.Add("270", "RETRY");
            dict.Add("300", "EMAIL_NOT_VALID");
            dict.Add("310", "DOMAIN_EXISTS_BUT_SPAM");
            foreach (var value in responses)
            {
                value.id = value.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete;
                if (value.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr != 220 && value.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr != 270)
                {
                    signal SignalObject = new signal();
                    Key keyobject = new Key();
                    List<Key> keyslist = new List<Key>();
                    keyobject.value = value.id;
                    keyslist.Add(keyobject);
                    SignalObject.keys = keyslist;
                    List<Attribute> attributelist = new List<Attribute>();
                    Attribute attributeobjectnew = new Attribute();
                    Attribute attributeobjectold = new Attribute();
                    Guid g = Guid.NewGuid();
                    SignalObject.correlationId = g.ToString();

                    attributeobjectnew.key = "EmailMatchType.new";
                    attributeobjectnew.value = dict[value.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr.ToString()];
                    attributeobjectold.key = "EmailMatchType.old";
                    attributeobjectold.value = dict["220"];
                    attributelist.Add(attributeobjectnew);
                    attributelist.Add(attributeobjectold);
                    SignalObject.attributes = attributelist;
                    SignalObject.originatingSystemDate = lastModified;
                    SignalObject.internalProcessingDate = DateTime.UtcNow;
                    value.partitionKey = value.id.Substring(0,2);
                    await SendSignalviaHttpAsync(SignalObject);
                    await p.AddingAndDeleting(value);
                }
            }
        }

        public static async Task SendSignalviaHttpAsync(signal signalObject)
        {
            var token = new AzureServiceTokenProvider("RunAs=App;AppId=aa0c3919-10cc-41aa-b236-35329c72ce95;TenantId=72f988bf-86f1-41af-91ab-2d7cd011db47;CertificateThumbprint=624225424959582dc202a153b69aa7f85c90c57b;CertificateStoreLocation=CurrentUser");
        var requiredtoken=  await token.GetAccessTokenAsync("https://activitystore-ppe.trafficmanager.net");
            HttpClient client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", requiredtoken);
            var json = JsonConvert.SerializeObject(signalObject);
            var data = new StringContent(json, Encoding.UTF8, "application/json");

            var signalurl = "https://activitystore-ppe.trafficmanager.net/signalacquisition-dev/api/v1/signal";

            var response = await client.PostAsync(signalurl, data);

            string result = await response.Content.ReadAsStringAsync();
            Console.WriteLine(result);
        }

        static void DownloadFileFromInformatica()
        {
            using (var sftp = new SftpClient("sftp.strikeiron.com", 22, "microsofttest", "Ch@!jYmW"))
            {
                sftp.Connect();

                using (var file = File.OpenWrite(@"C:\Users\Lenovo\Documents\emailvalidation_Processed.csv"))
                {
                    sftp.DownloadFile(@"/From_Strikeiron/emailvalidation_Processed.csv", file);
                }

                sftp.Disconnect();
            }
        }
        
        static getmainmail FromCsv(string csvLine)
        {
            string[] array = csvLine.Split(',');
            getmainmail getmainmailObj = new getmainmail();
            VerifyEmailResponse verifyemailresponse = new VerifyEmailResponse();
            VerifyEmailResult verifyemailres = new VerifyEmailResult();
            verifyemailresponse.VerifyEmailResult = verifyemailres;
            ServiceResult servresult = new ServiceResult();
            ServiceStatus servstatus = new ServiceStatus();
            Hygiene hygieneobj = new Hygiene();
            Email mailobject = new Email();
            Reason Reasonobj = new Reason();
            verifyemailres.ServiceStatus = servstatus;
            verifyemailres.ServiceResult = servresult;
            servresult.Reason = Reasonobj;
            servresult.Email = mailobject;
            servresult.Hygiene = hygieneobj;
            getmainmailObj.VerifyEmailResponse = verifyemailresponse;
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete = array[0];
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr = long.Parse(array[1]);
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusDescription = array[2];
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Reason.Code = long.Parse(array[3]);
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Reason.Description = array[4];
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Hygiene.HygieneResult = array[5];
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Hygiene.NetProtected = bool.Parse(array[6]);
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Hygiene.NetProtectedBy = array[7];
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.LocalPart = array[11];
            getmainmailObj.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.DomainPart = array[12];
            return getmainmailObj;
        }
    }
}

