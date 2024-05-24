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
    public class ReadGzip<T> where T : class
    {
        private long[] _gzipLengths;
        private long _gzipOffset;
        private byte[] _buffer;
        private long _total;
        private JsonDataTransform _jsonDataTransform;
        private FileStream _fileStream;
        private T instance;

        public ReadGzip(int BufferSize,FileStream fileStream, T parent)
        {
            long[] gzipLengths = ReadGzipLengths(fileStream);
            long gzipOffset = 0;
            var buffer = new byte[BufferSize];
            long total = 0;
            var jsonDataTransform = new JsonDataTransform();

            this._total = total;
            this._buffer = buffer;
            this._jsonDataTransform = jsonDataTransform;
            this._gzipLengths = gzipLengths;
            this._gzipOffset = gzipOffset;
            this._fileStream = fileStream;
            this.instance = parent;
        }
        public void ReadAndTrain(CancellationTokenSource tokenSource)
        {
            foreach (long gzipLength in _gzipLengths)
            {
                _fileStream.Position = _gzipOffset;

                using (var gz = new GZipStream(_fileStream, CompressionMode.Decompress, true))
                {
                    int bytesRead;

                    while ((bytesRead = gz.Read(_buffer, 0, _buffer.Length)) > 0)
                    {
                        if (!tokenSource.IsCancellationRequested)
                        {
                            instance.GetType().GetMethod("loopThrough").Invoke(instance, new Object[] { _jsonDataTransform, _buffer });
                            _total += bytesRead;
                        }
                        else
                        {
                            return;
                            //tokenSource.Token.ThrowIfCancellationRequested();
                        }
                    }
                }

                Console.WriteLine("Done");

                _gzipOffset += gzipLength;

                //Console.WriteLine("Uncompressed Bytes: {0:N0} ({1:N2} %)", total, gzipOffset * 100.0 / fileStream.Length);
            }

        }

        /// <summary>
        /// Read Gzip Lengths.
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        private long[] ReadGzipLengths(Stream stream)
        {
            int fieldBytes;

            // Check if can seek and can read and return if cannot.
            if (!stream.CanSeek || !stream.CanRead)
            {
                return null;
            }

            if (stream.ReadByte() == 0x1f && stream.ReadByte() == 0x8b &&                                   // Gzip magic-code.
                stream.ReadByte() == 0x08 &&                                                                // Deflate-mode.
                stream.ReadByte() == 0x04 &&                                                                // Flagged: has extra-field.
                stream.ReadByte() + stream.ReadByte() + stream.ReadByte() + stream.ReadByte() >= 0 &&       // Unix timestamp (ignored).
                stream.ReadByte() == 0x00 &&                                                                // Extra-flag: sould be zero
                stream.ReadByte() >= 0 &&                                                                   // OS-Type (ignored)
                (fieldBytes = stream.ReadByte() + stream.ReadByte() * 256 - 4) > 0 &&                       // Length of extra-field (subtract 4 bytes field-header).
                stream.ReadByte() == 0x53 && stream.ReadByte() == 0x5a &&                                   // Field-header: must be "SZ" (mean: gzip-sizes as uint32-values)
                stream.ReadByte() + stream.ReadByte() * 256 == fieldBytes)                                  // Should have same length.
            {
                var buffer = new byte[fieldBytes];

                if (stream.Read(buffer, 0, fieldBytes) == fieldBytes && fieldBytes % 4 == 0)
                {
                    var result = new long[fieldBytes / 4];

                    for (int i = 0; i < result.Length; i++)
                    {
                        result[i] = BitConverter.ToUInt32(buffer, i * sizeof(uint));
                    }

                    // Reset stream-position
                    stream.Position = 0;

                    return result;
                }
            }

            // Fallback for normal gzip-files or unknown structures.
            // Reset stream-position.
            stream.Position = 0;

            // Return single default-length.
            return new[] { stream.Length };
        }

    }
}
