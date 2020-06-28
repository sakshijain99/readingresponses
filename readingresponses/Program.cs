using cosmosdbquery;
using Microsoft.Azure.Cosmos;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public string signalType = "EmailValidationDataUpdate";
        public string action = "Update";
        public Key keys { get; set; }
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
            
            foreach (var value in responses)
            {
                value.Id = value.VerifyEmailResponse.VerifyEmailResult.ServiceResult.Email.Complete;
                if (value.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr != 220&& value.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr!=270)
                {
                    signal SignalObject = new signal();
                    Key keyobject = new Key();
                    keyobject.value = value.Id;
                    SignalObject.keys = keyobject;
                    List<Attribute> attributelist = new List<Attribute>();
                    Attribute attributeobjectnew = new Attribute();
                    Attribute attributeobjectold = new Attribute();
                    Guid g = Guid.NewGuid();
                    SignalObject.correlationId = g.ToString();
             
                    attributeobjectnew.key = "StatusCode.new";
                    attributeobjectnew.value = value.VerifyEmailResponse.VerifyEmailResult.ServiceStatus.StatusNbr.ToString();
                    attributeobjectold.key = "StatusCode.old";
                    attributeobjectold.value = "220";
                    attributelist.Add(attributeobjectnew);
                    attributelist.Add(attributeobjectold);
                    SignalObject.attributes = attributelist;
                    SignalObject.originatingSystemDate = lastModified;
                    SignalObject.internalProcessingDate = DateTime.UtcNow;
                   // SendSignalviaHttp(SignalObject);
                    await p.AddingAndDeleting(value);
                }
            }
        }

        public static void SendSignalviaHttp(signal signalObject)
        {
         
        }

        static void DownloadFileFromInformatica()
        {
            using (var sftp = new SftpClient("sftp.strikeiron.com", 22, "microsofttest","Ch@!jYmW"))
            {
                sftp.Connect();

                using (var file = File.OpenWrite(@"C:\Users\Lenovo\Documents\emailvalidation_Processed.csv"))
                {
                    sftp.DownloadFile(@"/From_Strikeiron/emailvalidation_Processed.csv", file);
                }

                sftp.Disconnect();
            }
        }
        public static async Task<string> GetOAuthTokenFromAAD(string clientId, string clientSecret, string resource)
        {
            ClientCredential clientCredential = new ClientCredential(clientId, clientSecret);

            AuthenticationContext authenticationContext = new AuthenticationContext("");

            Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationResult result = await authenticationContext.AcquireTokenAsync(resource, clientCredential);

            if (result == null)
            {
                throw new InvalidOperationException("Failed to obtain the JWT token");
            }

            return result.AccessToken;
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
