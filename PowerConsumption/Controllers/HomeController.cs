using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualBasic.FileIO;
using PwrConsFinal.Models;
using System.Data;
using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;
using Microsoft.VisualBasic;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using System.Net;

namespace PwrConsFinal.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
            con.ConnectionString = "Server=NOTEBOOK-RAJITH\\SQLEXPRESS;Database=PowerConsumption;Trusted_Connection=True;TrustServerCertificate=true";
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult AddCsv()
        {
            return View();
        }
        public IActionResult ViewAll()
        {
            return View();
        }

        public IActionResult OutputData()
        {
            FetchData();
            return View(data);
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        public IActionResult UploadCsv(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                ViewBag.ErrorMessage = "Please select a CSV file.";
                return View("Results");
            }

            try
            {
                using (var stream = file.OpenReadStream())
                using (TextFieldParser csvReader = new TextFieldParser(stream))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;

                    DataTable csvData = new DataTable();
                    string[] colFields = csvReader.ReadFields();

                    foreach (string column in colFields)
                    {
                        DataColumn datacolumn = new DataColumn(column);
                        datacolumn.AllowDBNull = false;
                        csvData.Columns.Add(datacolumn);
                    }

                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();

                        // Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }

                        csvData.Rows.Add(fieldData);
                    }

                    // Insert CSV data into SQL Server using SQL Bulk Copy
                    InsertDataIntoSQLServerUsingSQLBulkCopy(csvData);

                    ViewBag.SuccessMessage = "CSV data imported successfully.";

                }
            }
            catch (Exception ex)
            {
                ViewBag.ErrorMessage = "An error occurred: " + ex.Message;
            }

            return View("Results");

        }

        private void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData)
        {
            string connectionString = "Server=NOTEBOOK-RAJITH\\SQLEXPRESS;Database=PowerConsumption;Trusted_Connection=True;TrustServerCertificate=true";

            using (SqlConnection dbConnection = new SqlConnection(connectionString))
            {
                dbConnection.Open();

                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                {
                    s.DestinationTableName = "Data";

                    foreach (DataColumn column in csvFileData.Columns)
                        s.ColumnMappings.Add(column.ColumnName, column.ColumnName);

                    s.WriteToServer(csvFileData);

                }
            }
        }
        SqlCommand com = new SqlCommand();
        SqlDataReader reader;
        SqlConnection con = new SqlConnection();
        List<PwrConsData> data = new List<PwrConsData>();

        private void FetchData()
        {

            if (data.Count > 0) { data.Clear(); }
            else { data.Clear(); }
            try
            {
                con.Open();
                com.Connection = con;
                com.CommandText = "SELECT * from Data";
                reader = com.ExecuteReader();
                while (reader.Read())
                {
                    DateTime dateValue = (DateTime)reader["time"];
                    string date_str = dateValue.ToString("dd-MM-yyyy HH:mm:ss");

                    data.Add(new PwrConsData()
                    {
                        date_time = date_str,
                        QI_BL1_1PV = reader["QI_BL1_1PV"].ToString(),
                        QI_BL2_1PV = reader["QI_BL2_1PV"].ToString(),
                        QI_BL3_1PV = reader["QI_BL3_1PV"].ToString(),
                        QI_BL4_1PV = reader["QI_BL4_1PV"].ToString(),
                        QI_BLA_1PV = reader["QI_BLA_1PV"].ToString(),
                        QI_BL5_1PV = reader["QI_BL5_1PV"].ToString(),
                        QI_BL6_1PV = reader["QI_BL6_1PV"].ToString(),
                        QI_BL7_1PV = reader["QI_BL7_1PV"].ToString(),
                        QI_NTU_1PV = reader["QI_NTU_1PV"].ToString(),
                        QI_BP2_1PV = reader["QI_BP2_1PV"].ToString(),
                        QI_FUS_1PV = reader["QI_FUS_1PV"].ToString(),
                        QI_F2BPV = reader["QI_F2BPV"].ToString(),
                        QI_MUDPV = reader["QI_MUDPV"].ToString(),
                        QI_HOTPV = reader["QI_HOTPV"].ToString(),
                        QI_BP3PV = reader["QI_BP3PV"].ToString(),
                    });
                }
                con.Close();
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
    }
}