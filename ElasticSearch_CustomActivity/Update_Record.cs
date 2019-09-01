using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Elasticsearch.Net;
using Newtonsoft.Json;
using System.ComponentModel;

namespace Connector.ElasticSearch
{
    public class Update_Record : CodeActivity
    {
        [Category("Input")]
        [RequiredArgument]
        public InArgument<string> URL { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<string> JsonData { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<string> Index { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<string> Type { get; set; }

        [Category("Input")]
        [RequiredArgument]
        public InArgument<string> ID { get; set; }

        [Category("Authentication")]
        [RequiredArgument]
        public InArgument<bool> AuthenticationRequired { get; set; }

        [Category("Authentication")]
        public InArgument<String> UserName { get; set; }

        [Category("Authentication")]
        public InArgument<String> Password { get; set; }

        [Category("Output")]
        public OutArgument<String> ErrorMessage { get; set; }

        [Category("Output")]
        public OutArgument<bool> Result { get; set; }

        [Category("Output")]
        public OutArgument<String> Response { get; set; }


        protected override void Execute(CodeActivityContext context)
        {

            {
                String Error = "";
                bool Res = false;
                bool Success = false;
                String Resp = "";

                try
                {
                    var node = new Uri(URL.Get(context));
                    ConnectionConfiguration config;

                    if (AuthenticationRequired.Get(context) == true)
                    {
                        config = new ConnectionConfiguration(node).BasicAuthentication(UserName.Get(context), Password.Get(context));
                    }
                    else
                    {
                        config = new ConnectionConfiguration(node);
                    }

                    var lowlevelClient = new ElasticLowLevelClient(config);


                    string data = @"{""doc"": " + JsonData.Get(context) + "}";
                    Console.WriteLine(data);
                    var updateData = lowlevelClient.Update<BytesResponse>
                        (Index.Get(context), Type.Get(context), ID.Get(context), data);

                    var responseBytes = updateData.Body;
                    if (responseBytes == null)
                    {
                        responseBytes = updateData.ResponseBodyInBytes;
                    }

                    Resp = Encoding.UTF8.GetString(responseBytes, 0, responseBytes.Length);

                    Success = updateData.Success;

                    if (Success == true)
                    {
                        Error = "";
                        Res = true;

                        ErrorMessage.Set(context, Error);
                        Response.Set(context, Resp);
                        Result.Set(context, Res);

                    }
                    else
                    {
                        throw updateData.OriginalException;
                        //ErrorMessage = insertData.OriginalException.ToString();
                        //Result = false;
                    }

                }
                catch (Exception ex)
                {
                    Error = "Failed to update the data." + ex.Message;
                    Res = false;

                    ErrorMessage.Set(context, Error);
                    Response.Set(context, Resp);
                    Result.Set(context, Res);
                }
            }

        }
    }
}
