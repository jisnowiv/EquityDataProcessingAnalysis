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
                if (args[1].Equals("1"))
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
                    LoadPriceData();
                }
                else if (args[1].Equals("2"))
                {
                    Console.WriteLine("What would you like to calculate?");
                    Console.WriteLine("\t1 - 52 Week High");
                    Console.WriteLine("\t2 - 52 Week Low");
                    Console.WriteLine("\t3 - 52 Day Moving Avg");

                    string? procChoice = Console.ReadLine();

                    var proc_dict = new Dictionary<string, string>
                    { 
                        ["1"] = Constants.proc_52_week_high,
                        ["2"] = Constants.proc_52_week_low ,
                        ["3"] = Constants.proc_52_day_moving_avg
                    }; 


                    Console.WriteLine("What ticker would you like to check?");
                    string? ticker = Console.ReadLine();
                    
                    // $TODO confirm that ticker exists

                    if (procChoice != null && ticker != null)
                    {
                        if (procChoice.Equals("1"))
                        {
                            Get52WeekHigh(ticker);
                        }
                        else if (procChoice.Equals("2"))
                        {
                            Get52WeekLow(ticker);
                        }
                        else if (procChoice.Equals("3"))
                        {
                            Get52DayMovingAvg(ticker);
                        }
                        else
                        {
                            Console.WriteLine("Invalid choice.");
                        }
                    }
                }
                else
                {
                    Usage();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occured: {ex.Message}");
            }
            Update("Main", "Exiting Main");
        }

        static void Usage()
        {
            Console.WriteLine("Usage: program.exe [function]");
            Console.WriteLine("\tfunction: Which basic function do you want to perform?");
            Console.WriteLine("\t\t1 - Retrieve data and load into database");
            Console.WriteLine("\t\t2 - Call a stored procedure on the database");
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

                    for (int i = 1; i < lines.Count(); i++)
                    {
                        string line = lines.ElementAt(i);
                        line = line.Remove(line.Length - 1, 1);
                        string query = $"INSERT INTO Tickers" +
                            " (ticker_id, ticker_table, permaticker, ticker, ticker_name, exchange, isdelisted, category, cusips, siccode, sicsector, famasector, famaindustry, sector, industry, scalemarketcap, scalerevenue, relatedtickers, currency, loc, lastupdated, firstadded, firstpricedate, lastpricedate, firstquarter, lastquarter, secfilings, companysite) " +
                            "VALUES (@param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10, @param11, @param12, @param13, @param14, @param15, @param16, @param17, @param18, @param19, @param20, @param21, @param22, @param23, @param24, @param25, @param26, @param27, @param28);";

                        string[] tokens = line.Split(',');
                        Console.WriteLine(tokens.Length);

                        using (SqlCommand cmd = new SqlCommand(query, dbConn))
                        {
                            cmd.Parameters.AddWithValue("@param1", 1); // Ticker ID, should auto update in db
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
                            cmd.Parameters.AddWithValue("@param12", tokens[10]);
                            cmd.Parameters.AddWithValue("@param13", tokens[11]);
                            cmd.Parameters.AddWithValue("@param14", tokens[12]);
                            cmd.Parameters.AddWithValue("@param15", tokens[13]);
                            cmd.Parameters.AddWithValue("@param16", tokens[14]);
                            cmd.Parameters.AddWithValue("@param17", tokens[15]);
                            cmd.Parameters.AddWithValue("@param18", tokens[16]);
                            cmd.Parameters.AddWithValue("@param19", tokens[17]);
                            cmd.Parameters.AddWithValue("@param20", tokens[18]);
                            cmd.Parameters.AddWithValue("@param21", tokens[19]);
                            cmd.Parameters.AddWithValue("@param22", tokens[20]);
                            cmd.Parameters.AddWithValue("@param23", tokens[21]);
                            cmd.Parameters.AddWithValue("@param24", tokens[22]);
                            cmd.Parameters.AddWithValue("@param25", tokens[23]);
                            cmd.Parameters.AddWithValue("@param26", tokens[24]);
                            cmd.Parameters.AddWithValue("@param27", tokens[25]);
                            cmd.Parameters.AddWithValue("@param28", tokens[26]);

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

        public static string ValidateTicker(SqlConnection conn, string tickerName)
        {
            try
            {
                string sqlCheck = "SELECT ticker_id COUNT(1) FROM Tickers WHERE ticker = @tickerName";

                using (SqlCommand cmd = new SqlCommand(sqlCheck, conn))
                {
                    cmd.Parameters.AddWithValue("@tickerName", tickerName);
                    
                    using (SqlDataReader rdr = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            string val = rdr.GetString(0);
                            return val;
                        }
                        else
                        {
                            return "";
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Update("ValidateTicker", $"Exception: {e.Message}");
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
                    Update("LoadPriceData", "Database connection opened");

                    var lines = File.ReadLines(Constants.PricesCSV);
                    Update("LoadPriceData", "Price CSV read");

                    Update("LoadPriceData", "Beginning write queries");
                    for (int i = 1; i < 2/*lines.Count()*/; i++)
                    {
                        string line = lines.ElementAt(i);
                        line = line.Remove(line.Length - 1, 1);
                        
                        string query = $"INSERT INTO Prices (" +
                            "ticker_id, ticker, price_id, price_date, price_open, price_high, price_low, price_close, volume, closeadj, closeunadj, lastupdated) " +
                            "VALUES (@param1, @param2, 1, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10, @param11)";

                        string[] tokens = line.Split(',');
                        Console.WriteLine(tokens.Length);

                        
                        ticker_id = ValidateTicker(dbConn, tokens[0]);
                        if (ticker_id.equals(""))
                        {
                            throw new Exception($"No ticker with the name {tokens[0]}");
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

        public static void Get52WeekHigh(string ticker)
        {
            Update("Get52WeekHigh", "Generating arguments");
            Tuple<string, string> args = Tuple.Create("TickerID", ticker);
            List<Tuple<string, string>> argList = new List<Tuple<string, string>> { args };

            Update("Get52WeekHigh", "Calling stored proc");
            CallStoredProc(Constants.proc_52_week_high, argList);
        }

        public static void Get52WeekLow(string ticker)
        {
            Update("Get52WeekLow", "Generating arguments");
            Tuple<string, string> args = Tuple.Create("TickerID", ticker);
            List<Tuple<string, string>> argList = new List<Tuple<string, string>> { args };

            Update("Get52WeekLow", "Calling stored proc");
            CallStoredProc(Constants.proc_52_week_low, argList);
        }

        public static void Get52DayMovingAvg(string ticker)
        {
            Update("Get52DayMovingAvg", "Generating arguments");
            Tuple<string, string> args = Tuple.Create("TickerID", ticker);
            List<Tuple<string, string>> argList = new List<Tuple<string, string>> { args };

            Update("Get52DayMovingAvg", "Calling stored proc");
            CallStoredProc(Constants.proc_52_day_moving_avg, argList);
        }

        public static void CallStoredProc(string procName, List<Tuple<string, string>> args)
        {
            try 
            {
                using (SqlConnection conn = new SqlConnection(Constants.connectionstring))
                {
                    conn.Open();

                    SqlCommand cmd = new SqlCommand(procName, conn);
                    cmd.CommandType = CommandType.StoredProcedure;

                    for (int i = 0; i < args.Count; i++)
                    {
                        Tuple<string, string> curArg = args[i];
                        cmd.Parameters.Add(new SqlParameter(curArg.Item1, curArg.Item2));
                    }

                    using (SqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {}
                    }
                }
            }
            catch (Exception e)
            {
                Update("CallStoredProc", $"Exception: {e.Message}");
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

        public const string proc_52_week_high = "Get_52_Week_High_Price";
        public const string proc_52_week_low = "Get_52_Week_Low_Price";
        public const string proc_52_day_moving_avg = "Get_52_day_moving_average";
    }
}