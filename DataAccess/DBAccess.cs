using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace AmazonReviewRandom.DataAccess
{
    public class DBAccess: IDisposable
    {
        private bool disposedValue;
        private SqlConnection conn = null;

        private SqlCommand getRandomKeyCmd = null;
        private SqlCommand getMaxKeysCountCmd = null;
        private SqlCommand getOptionsCmd = null;
        private SqlParameter getOptionParamKeyId = null;

        public DBAccess(string connectionString)
        {
            conn = new SqlConnection(connectionString);
            conn.Open();
        }
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    conn.Close();
                    conn.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private SqlCommand GetProc(string procName)
        {
            SqlCommand command = new SqlCommand(procName, conn);
            command.CommandType = CommandType.StoredProcedure;
            return command;
        }

        private DBAccess AddOutputStringParam(SqlCommand cmd, string paramName, int size)
        {
            SqlParameter keyParam = new SqlParameter(paramName, SqlDbType.NVarChar, size);
            keyParam.Direction = ParameterDirection.Output;
            cmd.Parameters.Add(keyParam);
            return this;
        }
        public List<KeyValuePair<int,string>> GetUniqueKeys()
        {
            var keys = new List<KeyValuePair<int, string>>();

            SqlCommand command = new SqlCommand("SELECT KeyID, [Key] FROM Keys ORDER BY 1", conn);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                    keys.Add(new KeyValuePair<int,string>(reader.GetInt32(0), reader.GetString(1)));
            }

            return keys;
        }

        public int GetKeyID(string key)
        {
            SqlCommand command = new SqlCommand("SELECT KeyID FROM Keys WHERE Key = @Key", conn);
            command.Parameters.AddWithValue("@Key", key);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    // Console.WriteLine(String.Format("{0}", reader.GetInt32(0)));
                    return reader.GetInt32(0);
                }
            }
            return 0; // not found
        }
        private void ClearStagingTable()
        {
            (new SqlCommand("TRUNCATE TABLE #NewOptions", conn)).ExecuteNonQuery();
        }
        private void CreateOptionsStagingTable()
        {
            (new SqlCommand(
                "IF OBJECT_ID('tempdb..#NewOptions') IS NULL " +
                    "CREATE TABLE #NewOptions ([Key] nvarchar(100) NOT NULL, [Option] nvarchar(100) NOT NULL)", conn))
                .ExecuteNonQuery();
        }

        private string Truncate(string value, int maxLength)
        {
            if (string.IsNullOrEmpty(value)) return value;
            return value.Length <= maxLength ? value : value.Substring(0, maxLength);
        }
        public void AddKeysAndOptions(Dictionary<string, List<string>> chain)
        {
            int keys = 0;
            int options = 0;

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("Key", typeof(string)));
            tbl.Columns.Add(new DataColumn("Option", typeof(string)));

            // and use KeyID as an argument to sp_AddKeyOption instead of the Key name, but this should suffice for now
            foreach (var keyValuePair in chain)
            {
                keys++;
                foreach (string option in keyValuePair.Value)
                {
                    DataRow dr = tbl.NewRow();
                    dr[0] = Truncate(keyValuePair.Key, 100); // truncate to max length of strings in database
                    dr[1] = Truncate(option, 100);
                    tbl.Rows.Add(dr);
                    options++;
                }
            }

            Console.Write(String.Format("Inserting {0:n0} Keys and {1:n0} Options:", keys, options));

            CreateOptionsStagingTable();

            SqlBulkCopy objbulk = new SqlBulkCopy(conn);
            objbulk.DestinationTableName = "#NewOptions";
            foreach (DataColumn column in tbl.Columns)
                objbulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping(column.ColumnName, column.ColumnName));
            objbulk.WriteToServer(tbl);

            GetProc("sp_AddKeysAndOptionsFromStaging").ExecuteNonQuery();

            ClearStagingTable();
            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            Console.WriteLine(" writing {0} options/s", (int)(1000.0 * options / ts.TotalMilliseconds));
        }

        private void InitialiseGetRandomKeyCmd()
        {
            if (getOptionsCmd is null)
            {
                getRandomKeyCmd = new SqlCommand("SELECT TOP 1[Key] FROM Keys ORDER BY NEWID()", conn);
            }
        }

        public string GetRandomKey()
        {
            var randomKey = string.Empty;
            InitialiseGetRandomKeyCmd();
            randomKey = string.Format("{0}", getRandomKeyCmd.ExecuteScalar());
            return randomKey;
        }

        public KeyValuePair<int, string> GetRandomKeyValue()
        {
            SqlCommand command = GetProc("sp_GetRandomKey");
            SqlParameter keyIdParam = command.Parameters.Add("@Key", SqlDbType.NVarChar, 100);
            SqlParameter keyParam = command.Parameters.Add("@KeyID", SqlDbType.Int);
            keyIdParam.Direction = ParameterDirection.Output;
            keyParam.Direction = ParameterDirection.Output;

            command.ExecuteNonQuery();

            return new KeyValuePair<int, string>((int)keyParam.Value, (string)keyIdParam.Value);
        }

        private void InitialiseGetOptionsCmd()
        {
            if (getOptionsCmd is null)
            {
                getOptionsCmd = new SqlCommand("SELECT [Option] FROM Options WHERE KeyID = (SELECT KeyID FROM Keys WHERE [Key] = @Key) ORDER BY 1", conn);
                getOptionParamKeyId = getOptionsCmd.Parameters.Add("@Key", SqlDbType.NVarChar, 100);
            }
        }

        private void InitialiseGetMaxKeysCountCmd()
        {
            if (getMaxKeysCountCmd is null)
            {
                getMaxKeysCountCmd = new SqlCommand("SELECT COUNT(*) FROM Keys", conn);
            }
        }

        public int GetKeysMaxCount()
        {
            var maxKeysCount = -1;
            InitialiseGetMaxKeysCountCmd();
            maxKeysCount = (int)getMaxKeysCountCmd.ExecuteScalar();
            return maxKeysCount;
        }
		
        public List<string> GetOptions(string key)
        {
            InitialiseGetOptionsCmd();
            getOptionParamKeyId.Value = key;
            List<string> options = new List<string>();

            using (IDataReader reader = getOptionsCmd.ExecuteReader())
                while (reader.Read())
                    options.Add(reader.GetString(0));

            return options;
        }
		
        public void ClearData()
        {
            (new SqlCommand("EXEC sp_ClearTables", conn)).ExecuteNonQuery();
        }
		
    }
}
