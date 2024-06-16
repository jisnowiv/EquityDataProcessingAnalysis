using System;
using System.IO.Compression;
using System.Data.SqlClient;
using Microsoft.Data.SqlClient;
using System.Data;

namespace EquityDataProcessingAnalysis
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Update("Main", "Starting Main");
            try
            {
                if (!Directory.Exists(Constants.TickersDir))
                {
                    await DownloadZipAsync(Constants.TickersUrl, Constants.TickersZip);
                    ExtractZipFile(Constants.TickersZip, Constants.TickersDir);
                }

                if (!Directory.Exists(Constants.PricesDir))
                {
                    await DownloadZipAsync(Constants.PricesUrl, Constants.PricesZip);
                    ExtractZipFile(Constants.PricesZip, Constants.PricesDir);
                }

                LoadTickerData();
                //LoadPriceData();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occured: {ex.Message}");
            }
            Update("Main", "Exiting Main");
        }

        static async Task DownloadZipAsync(string url, string outputPath)
        {
            using (HttpClient client = new HttpClient())
            {
                Update("DownloadZipAsync", "DownloadZipAsync client initialized");
                Update("DownloadZipAsync", $"Downloading {url}");

                HttpResponseMessage response = await client.GetAsync(url);

                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception($"Failed to download file: {response.StatusCode}");
                }

                Update("DownloadZipAsync", "Response sucessful");
                byte[] fileBytes = await response.Content.ReadAsByteArrayAsync();

                await File.WriteAllBytesAsync(outputPath, fileBytes);
                Update("DownloadZipAsync", "Write to file completed.");
                Update("DownloadZipAsync", $"Written to {outputPath}");
            }
        }

        static void ExtractZipFile(string zipPath, string extractPath)
        {
            Update("ExtractZipFile", $"Extracting {zipPath}");
            if (!File.Exists(zipPath))
            {
                throw new FileNotFoundException($"The file {zipPath} does not exist.");
            }

            if (Directory.Exists(extractPath))
            {
                Update("ExtractZipFile", "Deleting existing directory");
                Directory.Delete(extractPath, true);
            }

            Update("ExtractZipFile", "Starting extraction");
            ZipFile.ExtractToDirectory(zipPath, extractPath);
            Update("ExtractZipFile", "Finished extraction");
        }

        public static void LoadTickerData()
        {
            try
            {
                using (SqlConnection dbConn = new SqlConnection(Constants.connectionstring))
                {
                    dbConn.Open();
                    Console.WriteLine(dbConn.Database);

                    var lines = File.ReadLines(Constants.TickersCSV);

                    for (int i = 1; i < 2/*lines.Count()*/; i++)
                    {
                        string line = lines.ElementAt(i);
                        line = line.Remove(line.Length - 1, 1);
                        string query = $"INSERT INTO Tickers" +
                            " (ticker_table, permaticker, ticker, ticker_name, exchange, isdelisted, category, cusips, siccode, sicsector, famasector, famaindustry, sector, industry, scalemarketcap, scalerevenue, relatedtickers, currency, loc, lastupdated, firstadded, firstpricedate, lastpricedate, firstquarter, lastquarter, secfilings, companysite) " +
                            "VALUES (@param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10, @param11, @param12, @param13, @param14, @param15, @param16, @param17, @param18, @param19, @param20, @param21, @param22, @param23, @param24, @param25, @param26, @param27);";

                        string[] tokens = line.Split(',');
                        Console.WriteLine(tokens.Length);

                        using (SqlCommand cmd = new SqlCommand(query, dbConn))
                        {
                            cmd.Parameters.AddWithValue("@param1", tokens[0]);
                            cmd.Parameters.AddWithValue("@param2", tokens[1]);
                            cmd.Parameters.AddWithValue("@param3", tokens[2]);
                            cmd.Parameters.AddWithValue("@param4", tokens[3]);
                            cmd.Parameters.AddWithValue("@param5", tokens[4]);
                            cmd.Parameters.AddWithValue("@param6", tokens[5]);
                            cmd.Parameters.AddWithValue("@param7", tokens[6]);
                            cmd.Parameters.AddWithValue("@param8", tokens[7]);
                            cmd.Parameters.AddWithValue("@param9", tokens[8]);
                            cmd.Parameters.AddWithValue("@param10", tokens[9]);
                            cmd.Parameters.AddWithValue("@param11", tokens[10]);
                            cmd.Parameters.AddWithValue("@param12", tokens[11]);
                            cmd.Parameters.AddWithValue("@param13", tokens[12]);
                            cmd.Parameters.AddWithValue("@param14", tokens[13]);
                            cmd.Parameters.AddWithValue("@param15", tokens[14]);
                            cmd.Parameters.AddWithValue("@param16", tokens[15]);
                            cmd.Parameters.AddWithValue("@param17", tokens[16]);
                            cmd.Parameters.AddWithValue("@param18", tokens[17]);
                            cmd.Parameters.AddWithValue("@param19", tokens[18]);
                            cmd.Parameters.AddWithValue("@param20", tokens[19]);
                            cmd.Parameters.AddWithValue("@param21", tokens[20]);
                            cmd.Parameters.AddWithValue("@param22", tokens[21]);
                            cmd.Parameters.AddWithValue("@param23", tokens[22]);
                            cmd.Parameters.AddWithValue("@param24", tokens[23]);
                            cmd.Parameters.AddWithValue("@param25", tokens[24]);
                            cmd.Parameters.AddWithValue("@param26", tokens[25]);
                            cmd.Parameters.AddWithValue("@param27", tokens[26]);

                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Update("LoadTickerData", $"Exception: {e.Message}");
                throw;
            }
        }

        public static void LoadPriceData()
        {
            try
            {
                using (SqlConnection dbConn = new SqlConnection(Constants.connectionstring))
                {
                    dbConn.Open();
                    Console.WriteLine(dbConn.Database);

                    var lines = File.ReadLines(Constants.PricesCSV);

                    for (int i = 1; i < 2/*lines.Count()*/; i++)
                    {
                        string line = lines.ElementAt(i);
                        line = line.Remove(line.Length - 1, 1);
                        
                        string query = $"INSERT INTO Prices (" +
                            "ticker_id, ticker, price_date, price_open, price_high, price_low, price_close, volume, closeadj, closeunadj, lastupdated) " +
                            "VALUES (@param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10, @param11)";

                        string[] tokens = line.Split(',');
                        Console.WriteLine(tokens.Length);

                        string ticker_query = $"SELECT ticker_id FROM Tickers WHERE ticker = {tokens[0]}";
                        string ticker_id = "0";
                        using (SqlCommand cmd = new SqlCommand(ticker_query, dbConn))
                        {
                            SqlDataReader reader = cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                ticker_id = reader.GetString(0);
                            }
                        }

                        using (SqlCommand cmd = new SqlCommand(query, dbConn))
                        {
                            cmd.Parameters.AddWithValue("@param1", ticker_id);
                            cmd.Parameters.AddWithValue("@param2", tokens[0]);
                            cmd.Parameters.AddWithValue("@param3", tokens[1]);
                            cmd.Parameters.AddWithValue("@param4", tokens[2]);
                            cmd.Parameters.AddWithValue("@param5", tokens[3]);
                            cmd.Parameters.AddWithValue("@param6", tokens[4]);
                            cmd.Parameters.AddWithValue("@param7", tokens[5]);
                            cmd.Parameters.AddWithValue("@param8", tokens[6]);
                            cmd.Parameters.AddWithValue("@param9", tokens[7]);
                            cmd.Parameters.AddWithValue("@param10", tokens[8]);
                            cmd.Parameters.AddWithValue("@param11", tokens[9]);

                            cmd.CommandType = CommandType.Text;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Update("LoadPriceData", $"Exception: {e.Message}");
                throw;
            }
        }

        public static void Update(string func, string msg)
        {
#if DEBUG
            String timestamp = DateTime.Now.ToString();
            Console.WriteLine($"EDPA {func} {timestamp} {msg}");
#endif
        }
    }

    public static class Constants
    {
        public const string TickersUrl = "https://www.alphaforge.net/A0B1C3/TICKERS.zip";
        public const string TickersZip = "tickers.zip";
        public const string TickersDir = "tickers";
        public const string TickersCSV = "tickers/tickers.csv";

        public const string PricesUrl = "https://www.alphaforge.net/A0B1C3/PRICES.zip";
        public const string PricesZip = "prices.zip";
        public const string PricesDir = "prices";
        public const string PricesCSV = "prices/prices.csv";

        public const string connectionstring = "Data Source=GENERAL003;Initial Catalog=EDPA_DB;Integrated Security=True;Trust Server Certificate=True";
    }
}