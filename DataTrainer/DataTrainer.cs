using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AmazonReviewRandom.DataAccess;

namespace AmazonReviewRandom.DataTrainer
{
    public class DataTrainer
    {
        /// <summary>
        /// Buffer size for reading.
        /// </summary>
        private int BufferSize = 0;

        /// <summary>
        /// Name of Gzip file containing the data.
        /// </summary>
        private string GzipFileName = string.Empty;

        private string ConnectionString = string.Empty;

        /// <summary>
        /// Data trainer constructor.
        /// </summary>
        /// <param name="gzipFileName"></param>
        /// <param name="bufferSize"></param>
        public DataTrainer(string gzipFileName, int bufferSize, string connectionString)
        {
            BufferSize = bufferSize;

            GzipFileName = gzipFileName;

            ConnectionString = connectionString;
        }

        public DataTrainer(string connectionString)
        {
            ConnectionString = connectionString;
        }

        private string Join(string a, string b)
        {
            return a + " " + b;
        }

        public string GenerateReview(int keySize, int outputSize)
        {
            try
            {
                using (DBAccess db = new DBAccess(ConnectionString))
                {
                    var keysCount = db.GetKeysMaxCount();
                    var prefix = db.GetRandomKeyValue().Value; //initial call
                    if (string.IsNullOrEmpty(prefix))
                    {
                        return string.Empty;
                    }

                    Random rand = new Random();
                    List<string> output = new List<string>();
                    int n = 0;
                    int rn = rand.Next(keysCount);
                    output.AddRange(prefix.Split());

                    while (true)
                    {
                        var suffix = db.GetOptions(prefix);
                        if (suffix.Count == 1)
                        {
                            if (suffix[0] == "")
                            {
                                return output.Aggregate(Join);
                            }
                            output.Add(suffix[0]);
                        }
                        else
                        {
                            rn = rand.Next(suffix.Count);
                            output.Add(suffix[rn]);
                        }
                        if (output.Count >= outputSize)
                        {
                            return output.Take(outputSize).Aggregate(Join);
                        }
                        n++;
                        prefix = output.Skip(n).Take(keySize).Aggregate(Join);
                    }

                }
            }
            catch(Exception ex)
            {
                return "Failed to generate a review.";
            }
        }

        public void ClearData()
        {
            try
            {
                using (DBAccess db = new DBAccess(ConnectionString))
                {
                    db.ClearData();
                }
            }
            catch (Exception ex)
            {
                return;
            }
        }

        public void TrainData(CancellationTokenSource tokenSource)
        {
            try
            {
                using (var fileStream = File.OpenRead(GzipFileName))
                {
                    using (DBAccess db = new DBAccess(ConnectionString))
                    {
#if DEBUG
                        var readGzip = new ReadGzip<DataTrainer>(BufferSize, fileStream, this);
                        readGzip.ReadAndTrain(tokenSource);
#else
                    new ReadGzip<DataTrainer>(BufferSize, fileStream, this).AddintoDal();
#endif

                    }
                }
            }
            catch (Exception ex)
            {
                return;
            }
        }

        public void loopThrough(JsonDataTransform jsonDataTransform, byte[] buffer)
        {
            try
            {
                using (DBAccess db = new DBAccess(ConnectionString))
                {
#if DEBUG
                    var chainDictionary = new MarkovChain().GetMarkOvChainDictionary(jsonDataTransform, buffer);
                    db.AddKeysAndOptions(chainDictionary);
#else
                _db.AddKeysAndOptions(new MarkovChain().GetMarkOvChainDictionary(_jsonDataTransform, _buffer));
#endif
                }
            }
            catch (Exception ex)
            {
                return;
            }

        }

    }
}
