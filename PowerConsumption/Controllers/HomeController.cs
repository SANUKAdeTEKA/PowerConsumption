using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualBasic.FileIO;
using PwrConsFinal.Models;
using System.Data;
using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using System;
using System.Data.SqlClient;
using Microsoft.VisualBasic;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using System.Net;
using PowerConsumption.Models;
using static System.Runtime.InteropServices.JavaScript.JSType;
using Microsoft.AspNetCore.Http.HttpResults;

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





        /*

                /// <summary>
                ///   Finds the peaks of a signal.
                /// </summary>
                /// 
                /// <param name="samples">The samples.</param>
                /// 
                /// <returns>The index of the peaks found in the sample.</returns>
                /// 
                public static int[] FindPeaks(this double[] samples)
                {
                    var peaks = new List<int>();

                    for (int i = 1; i < samples.Length - 1; i++)
                    {
                        if (samples[i] > samples[i - 1] && samples[i] > samples[i + 1])
                            peaks.Add(i);
                    }

                    return peaks.ToArray();
                }




                public class PeakController : Controller
                {
                    private readonly YourDbContext _context;

                    public PeakController(YourDbContext context)
                    {
                        _context = context;
                    }

                    public IActionResult DisplayPeaks(int selectedValue)
                    {


                        // Query the database to retrieve the relevant data
                        var data = _context.Data.Where(x => x.SomeColumn == selectedValue).ToList();

                        // Calculate peak values
                        var peakValues = new List<double>();
                        for (int i = 1; i < data.Count - 1; i++)
                        {
                            if (data[i] > data[i - 1] && data[i] > data[i + 1])
                            {
                                peakValues.Add(data[i]);
                            }
                        }

                        var model = new PeakModel
                        {
                            PeakValues = peakValues
                        };

                        return View(model);
                    }
                }

        */


        private ApplicationDbContext db = new ApplicationDbContext();

        public ActionResult Index2()
        {
            return View();
        }

        [HttpPost]
        public ActionResult CalculateSum(string input)
        {
            string connectionString = "Server=NOTEBOOK-RAJITH\\SQLEXPRESS;Database=PowerConsumption;Trusted_Connection=True;TrustServerCertificate=true";

            // Create a dictionary to store the results of multiple queries
            Dictionary<string, int> results = new Dictionary<string, int>();

            if (!string.IsNullOrEmpty(input))
            {
                switch (input)
                {
                    case "QI_BL1_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BL2_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BL3_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BL4_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BLA_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BL5_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BL6_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BL7_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_NTU_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BP2_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_FUS_1PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_F2BPV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_MUDPV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_HOTPV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");

                        break;

                    case "QI_BP3PV":

                        results["Hour01"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-03 00:59'");
                        results["Hour02"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 01:00' AND time < '2011-04-03 01:59'");
                        results["Hour03"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 02:00' AND time < '2011-04-03 02:59'");
                        results["Hour04"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 03:00' AND time < '2011-04-03 03:59'");
                        results["Hour05"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 04:00' AND time < '2011-04-03 04:59'");
                        results["Hour06"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 05:00' AND time < '2011-04-03 05:59'");
                        results["Hour07"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 06:00' AND time < '2011-04-03 06:59'");
                        results["Hour08"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 07:00' AND time < '2011-04-03 07:59'");
                        results["Hour09"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 08:00' AND time < '2011-04-03 08:59'");
                        results["Hour10"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 09:00' AND time < '2011-04-03 09:59'");
                        results["Hour11"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 10:00' AND time < '2011-04-03 10:59'");
                        results["Hour12"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 11:00' AND time < '2011-04-03 11:59'");
                        results["Hour13"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 12:00' AND time < '2011-04-03 12:59'");
                        results["Hour14"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 13:00' AND time < '2011-04-03 13:59'");
                        results["Hour15"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 14:00' AND time < '2011-04-03 14:59'");
                        results["Hour16"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 15:00' AND time < '2011-04-03 15:59'");
                        results["Hour17"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 16:00' AND time < '2011-04-03 16:59'");
                        results["Hour18"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 17:00' AND time < '2011-04-03 17:59'");
                        results["Hour19"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 18:00' AND time < '2011-04-03 18:59'");
                        results["Hour20"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 19:00' AND time < '2011-04-03 19:59'");
                        results["Hour21"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 20:00' AND time < '2011-04-03 20:59'");
                        results["Hour22"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 21:00' AND time < '2011-04-03 21:59'");
                        results["Hour23"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 22:00' AND time < '2011-04-03 22:59'");
                        results["Hour24"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 23:00' AND time < '2011-04-03 23:59'");
                        results["Total"] = ExecuteQuery("SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '2011-04-03 00:00' AND time < '2011-04-04 00:00'");


                        break;

                }

            }
            ViewBag.SelectedInput = input;
            ViewBag.Results = results;
            return View("ViewAll");
        }

        /*

                public ActionResult CalculateSum(string input)
                {
                    string connectionString = "Server=NOTEBOOK-RAJITH\\SQLEXPRESS;Database=PowerConsumption;Trusted_Connection=True;TrustServerCertificate=true";

                    // Create a dictionary to store the results of multiple queries
                    Dictionary<string, int> results = new Dictionary<string, int>();

                    int sum = 0;
                    if (!string.IsNullOrEmpty(input))
                    {
                        DateTime now = DateTime.Now;
                        DateTime startDateTime = now.AddHours(-24); // Calculate the start time (24 hours ago)

                        //string sqlQuery = string.Empty;
                        switch (input)
                        {
                            case "QI_BL1_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL1_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL1_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BL2_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL2_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL2_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BL3_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL3_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL3_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BL4_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL4_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL4_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BLA_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BLA_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BLA_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BL5_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL5_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL5_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BL6_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL6_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL6_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BL7_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BL7_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BL7_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_NTU_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_NTU_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_NTU_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BP2_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BP2_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BP2_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_FUS_1PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_FUS_1PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_FUS_1PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_F2BPV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_F2BPV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_F2BPV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_MUDPV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_MUDPV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_MUDPV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_HOTPV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_HOTPV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_HOTPV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                            case "QI_BP3PV":
                                for (int i = 0; i < 24; i++)
                                {
                                    DateTime fromTime = startDateTime.AddHours(i);
                                    DateTime toTime = startDateTime.AddHours(i + 1);
                                    string key = $"QI_BP3PV-{i + 1:D2}";

                                    string sqlQuery = $"SELECT SUM(QI_BP3PV) FROM Data WHERE time >= '{fromTime:yyyy-MM-dd HH:mm}' AND time < '{toTime:yyyy-MM-dd HH:mm}'";
                                    results[key] = ExecuteQuery(sqlQuery);
                                }
                                break;

                        }

                    }
                    ViewBag.Results = results;
                    // Return a partial view that contains the results as HTML
                    //return PartialView("_ResultsPartialView");
                    return View("ViewAll");
                }

       


        */



        private int ExecuteQuery(string sqlQuery)
        {
            string connectionString = "Server=NOTEBOOK-RAJITH\\SQLEXPRESS;Database=PowerConsumption;Trusted_Connection=True;TrustServerCertificate=true";

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(sqlQuery, connection))
                {
                    var result = command.ExecuteScalar();

                    if (Convert.IsDBNull(result) || result == null)
                    {
                        return 0;
                    }
                    else
                    {
                        return Convert.ToInt32(result);
                    }
                }
            }
        }

        /*
         
        public ActionResult ViewAll()
        {
            double[] samples = // Retrieve your double[] from the database based on the selected value from the dropdown list.  ;

            int[] peaks = samples.FindPeaks(); // Call the FindPeaks extension method

            // Pass the peaks array to the view
            ViewBag.Peaks = peaks;

            return View();
        }

        
    }
    
        

    

    public static class PeakExtensions
    {
        public static int[] FindPeaks(this double[] samples)
        {
            var peaks = new List<int>();

            for (int i = 1; i < samples.Length - 1; i++)
            {
                if (samples[i] > samples[i - 1] && samples[i] > samples[i + 1])
                {
                    bool isPeak = true;

                    // Check if it's a peak based on your definition
                    for (int j = i - 1; j >= 0; j--)
                    {
                        if (samples[j] >= samples[i])
                        {
                            isPeak = false;
                            break;
                        }
                    }

                    if (isPeak)
                    {
                        for (int j = i + 1; j < samples.Length; j++)
                        {
                            if (samples[j] >= samples[i])
                            {
                                isPeak = false;
                                break;
                            }
                        }
                    }

                    if (isPeak)
                    {
                        peaks.Add(i);
                    }
                }
            }

            return peaks.ToArray();
        }
    }

     */

    }
}

