using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Microsoft.AspNetCore.Hosting;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Aspnet_Bigquery.Bigquery
{
    public class BQ : IBQ
    {
        const string PROJECT_ID = "YOUR_PROJECT_ID";

        readonly IHostingEnvironment _hostingEnvironment;

        public BQ(IHostingEnvironment hostingEnvironment)
        {
            _hostingEnvironment = hostingEnvironment;
        }

        public BigQueryClient GetBigqueryClient()
        {
            var config = Path.Combine(_hostingEnvironment.WebRootPath, "bq-secrets.json");

            GoogleCredential credential = null;
            using (var jsonStream = new FileStream(config, FileMode.Open, FileAccess.Read, FileShare.Read))
                credential = GoogleCredential.FromStream(jsonStream);

            return BigQueryClient.Create(PROJECT_ID, credential);
        }

        public List<TableRow> GetRows(string query)
        {
            var bqClient = GetBigqueryClient();

            var response = new List<TableRow>();

            var jobResource = bqClient.Service.Jobs;
            var qr = new QueryRequest() { Query = query };

            var queryResponse = jobResource.Query(qr, PROJECT_ID).Execute();

            if (queryResponse.JobComplete != false)
            {
                return queryResponse.Rows == null
                    ? new List<TableRow>()
                    : queryResponse.Rows.ToList();
            }

            var jobId = queryResponse.JobReference.JobId;

            var retry = true;
            var retryCounter = 0;
            while (retry && retryCounter < 50)
            {
                Thread.Sleep(1000);

                var queryResults = bqClient.Service.Jobs.GetQueryResults(PROJECT_ID, jobId).Execute();

                if (queryResults.JobComplete != true)
                {
                    retryCounter++;
                    continue;
                }

                if (queryResults.Rows != null)
                    response = queryResults.Rows.ToList();

                retry = false;
            }

            return response;
        }
    }
}
