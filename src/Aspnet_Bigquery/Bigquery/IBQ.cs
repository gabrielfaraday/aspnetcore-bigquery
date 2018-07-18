using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System.Collections.Generic;

namespace Aspnet_Bigquery.Bigquery
{
    public interface IBQ
    {
        List<TableRow> GetRows(string query);
        BigQueryClient GetBigqueryClient();
    }
}
