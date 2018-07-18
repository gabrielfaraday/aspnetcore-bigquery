using System;
using System.Collections.Generic;
using System.Linq;
using Aspnet_Bigquery.Bigquery;
using Aspnet_Bigquery.Models;
using Google.Cloud.BigQuery.V2;
using Microsoft.AspNetCore.Mvc;

namespace Aspnet_Bigquery.Controllers
{
    [Route("api/big-query")]
    public class BQController : Controller
    {
        private const int ORDER_ID = 0;
        private const int ORDER_CATEGORY = 1;
        private const int ORDER_STATUS = 2;

        private readonly IBQ _bigQuery;

        public BQController(IBQ bigQuery)
        {
            _bigQuery = bigQuery;
        }

        [HttpPost]
        public IActionResult Post([FromBody]List<File> files)
        {
            var client = _bigQuery.GetBigqueryClient();

            var dataset = client.GetOrCreateDataset("YOUR DATASET");

            var table = dataset.GetOrCreateTable("YOUR TABLE", new TableSchemaBuilder
            {
                { "arquivoid", BigQueryDbType.String },
                { "categoria", BigQueryDbType.String },
                { "status", BigQueryDbType.String }
            }.Build());

            var bqRows = new List<BigQueryInsertRow>();
            files.ToList().ForEach(f => bqRows.Add
            (
                new BigQueryInsertRow
                {
                    { "arquivoid", Guid.NewGuid().ToString() },
                    { "categoria", f.Category },
                    { "status", f.Status }
                }));

            if (bqRows.Count > 1)
                table.InsertRows(bqRows);
            else if (bqRows.Count == 1)
                table.InsertRow(bqRows[0]);

            return Ok($"{bqRows.Count} rows have been added.");
        }

        [HttpGet]
        public IActionResult Get()
        {
            var query = "YOUR QUERY HERE, FOLLOWING THE FIELDS ORDER DEFINED ABOVE";

            var rows = _bigQuery.GetRows(query);

            var result = new List<File>();
            rows.ForEach(row => result.Add(new File
            {
                Id = row.F[ORDER_ID].V.ToString(),
                Category = row.F[ORDER_CATEGORY].V.ToString(),
                Status = row.F[ORDER_STATUS].V.ToString()
            }));

            return Ok(result);
        }
    }
}
