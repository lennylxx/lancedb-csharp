namespace CloudSample
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Arrow;

    using lancedb;

    /// <summary>
    /// End-to-end check that the cloud object store backends are wired up
    /// in the C# SDK. Prompts for a vendor and credentials, connects to a
    /// cloud URI, creates a table, writes rows, and reads them back.
    ///
    /// Supported vendors: AWS S3, Azure Blob, Google Cloud Storage,
    /// Alibaba OSS, Hugging Face Hub (read-only).
    ///
    /// The bucket / container / repo must already exist.
    /// </summary>
    public static class Program
    {
        public static async Task<int> Main()
        {
            try
            {
                var vendor = ChooseVendor();
                var (uri, storageOptions, readOnly) = vendor switch
                {
                    Vendor.S3 => ConfigureS3(),
                    Vendor.S3WithDynamoDb => ConfigureS3WithDynamoDb(),
                    Vendor.Azure => ConfigureAzure(),
                    Vendor.Gcs => ConfigureGcs(),
                    Vendor.Oss => ConfigureOss(),
                    Vendor.HuggingFace => ConfigureHuggingFace(),
                    _ => throw new InvalidOperationException("unknown vendor"),
                };

                Console.WriteLine();
                Console.WriteLine($"URI:       {uri}");
                Console.WriteLine($"Mode:      {(readOnly ? "read-only" : "read/write")}");
                Console.WriteLine();

                if (readOnly)
                {
                    return await RunReadOnly(uri, storageOptions);
                }
                return await RunReadWrite(uri, storageOptions);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"\nFAILED: {ex}");
                return 1;
            }
        }

        private static async Task<int> RunReadWrite(string uri, Dictionary<string, string> storageOptions)
        {
            const string tableName = "test_table";

            Console.Write("Connecting... ");
            var db = new Connection();
            await db.Connect(uri, new ConnectionOptions { StorageOptions = storageOptions });
            Console.WriteLine("OK");

            // Idempotent: drop any leftover table from a previous run.
            await db.DropTable(tableName, ignoreMissing: true);

            Console.Write("Creating table... ");
            var schema = new Schema.Builder()
                .Field(new Field("id", Apache.Arrow.Types.Int32Type.Default, false))
                .Field(new Field("name", Apache.Arrow.Types.StringType.Default, false))
                .Build();
            var idArray = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
            var nameArray = new StringArray.Builder().Append("alice").Append("bob").Append("carol").Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { idArray, nameArray }, 3);

            using (var table = await db.CreateTable(tableName, new CreateTableOptions
            {
                Data = new[] { batch },
                StorageOptions = storageOptions,
            }))
            {
                Console.WriteLine("OK");

                Console.Write("Counting rows... ");
                long count = await table.CountRows();
                Assert(count == 3, $"Expected 3 rows, got {count}");
                Console.WriteLine($"OK ({count})");

                Console.Write("Listing tables... ");
                var tables = await db.TableNames();
                Assert(tables.Contains(tableName), $"Expected table list to contain '{tableName}'");
                Console.WriteLine($"OK ({string.Join(", ", tables)})");
            }

            Console.Write("Re-opening table... ");
            using (var reopened = await db.OpenTable(tableName, new OpenTableOptions
            {
                StorageOptions = storageOptions,
            }))
            {
                long count = await reopened.CountRows();
                Assert(count == 3, $"Expected 3 rows after reopen, got {count}");
                Console.WriteLine($"OK ({count})");
            }

            db.Close();

            Console.WriteLine();
            Console.WriteLine("Cloud backend works end-to-end.");
            Console.WriteLine($"Files left at {uri} for inspection.");
            return 0;
        }

        private static async Task<int> RunReadOnly(string uri, Dictionary<string, string> storageOptions)
        {
            string tableName = Prompt("Existing table name", "");
            if (string.IsNullOrEmpty(tableName))
            {
                Console.Error.WriteLine("Read-only mode requires an existing table name.");
                return 2;
            }

            Console.Write("Connecting... ");
            var db = new Connection();
            await db.Connect(uri, new ConnectionOptions { StorageOptions = storageOptions });
            Console.WriteLine("OK");

            Console.Write("Listing tables... ");
            var tables = await db.TableNames();
            Console.WriteLine($"OK ({string.Join(", ", tables)})");

            Console.Write($"Opening '{tableName}'... ");
            using (var table = await db.OpenTable(tableName, new OpenTableOptions
            {
                StorageOptions = storageOptions,
            }))
            {
                Console.WriteLine("OK");

                Console.Write("Counting rows... ");
                long count = await table.CountRows();
                Console.WriteLine($"OK ({count})");
            }

            db.Close();

            Console.WriteLine();
            Console.WriteLine("Cloud backend works for read-only access.");
            return 0;
        }

        private enum Vendor
        {
            S3,
            S3WithDynamoDb,
            Azure,
            Gcs,
            Oss,
            HuggingFace,
        }

        private static Vendor ChooseVendor()
        {
            Console.WriteLine("Select cloud vendor:");
            Console.WriteLine("  1) AWS S3                    (s3://)");
            Console.WriteLine("  2) AWS S3 + DynamoDB commits (s3+ddb://)");
            Console.WriteLine("  3) Azure Blob Storage        (az://)");
            Console.WriteLine("  4) Google Cloud Storage      (gs://)");
            Console.WriteLine("  5) Alibaba OSS               (oss://)");
            Console.WriteLine("  6) Hugging Face Hub          (hf://, read-only)");

            while (true)
            {
                Console.Write("Choice [1-6]: ");
                string? input = Console.ReadLine()?.Trim();
                switch (input)
                {
                    case "1": return Vendor.S3;
                    case "2": return Vendor.S3WithDynamoDb;
                    case "3": return Vendor.Azure;
                    case "4": return Vendor.Gcs;
                    case "5": return Vendor.Oss;
                    case "6": return Vendor.HuggingFace;
                    default: Console.WriteLine("Invalid choice."); break;
                }
            }
        }

        private static (string uri, Dictionary<string, string> options, bool readOnly) ConfigureS3()
        {
            string bucket = Prompt("S3 bucket", "");
            string region = Prompt("AWS region", "us-east-1");
            string accessKeyId = Prompt("AWS access key id", "");
            string secretAccessKey = PromptSecret("AWS secret access key");
            string prefix = Prompt("Path prefix inside bucket", "lancedb-sample-db");

            var options = new Dictionary<string, string>
            {
                { "aws_access_key_id", accessKeyId },
                { "aws_secret_access_key", secretAccessKey },
                { "aws_region", region },
            };
            return ($"s3://{bucket}/{prefix}", options, false);
        }

        private static (string uri, Dictionary<string, string> options, bool readOnly) ConfigureS3WithDynamoDb()
        {
            string bucket = Prompt("S3 bucket", "");
            string region = Prompt("AWS region", "us-east-1");
            string accessKeyId = Prompt("AWS access key id", "");
            string secretAccessKey = PromptSecret("AWS secret access key");
            string prefix = Prompt("Path prefix inside bucket", "lancedb-sample-db");
            string ddbTable = Prompt("DynamoDB table name (must exist; hash=base_uri str, range=version num)", "");

            var options = new Dictionary<string, string>
            {
                { "aws_access_key_id", accessKeyId },
                { "aws_secret_access_key", secretAccessKey },
                { "aws_region", region },
            };
            return ($"s3+ddb://{bucket}/{prefix}?ddbTableName={ddbTable}", options, false);
        }

        private static (string uri, Dictionary<string, string> options, bool readOnly) ConfigureAzure()
        {
            string account = Prompt("Storage account name", "");
            string container = Prompt("Container name", "lancedb-sample");
            string key = PromptSecret("Storage account key");
            string prefix = Prompt("Path prefix inside container", "lancedb-sample-db");

            var options = new Dictionary<string, string>
            {
                { "azure_storage_account_name", account },
                { "azure_storage_account_key", key },
            };
            return ($"az://{container}/{prefix}", options, false);
        }

        private static (string uri, Dictionary<string, string> options, bool readOnly) ConfigureGcs()
        {
            string bucket = Prompt("GCS bucket", "");
            string saPath = Prompt("Service account JSON file path", "");
            string prefix = Prompt("Path prefix inside bucket", "lancedb-sample-db");

            if (!File.Exists(saPath))
            {
                throw new FileNotFoundException($"Service account JSON not found: {saPath}");
            }

            var options = new Dictionary<string, string>
            {
                { "google_service_account_key", File.ReadAllText(saPath) },
            };
            return ($"gs://{bucket}/{prefix}", options, false);
        }

        private static (string uri, Dictionary<string, string> options, bool readOnly) ConfigureOss()
        {
            string bucket = Prompt("OSS bucket", "");
            string endpoint = Prompt("OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com)", "");
            string accessKeyId = Prompt("OSS access key id", "");
            string accessKeySecret = PromptSecret("OSS access key secret");
            string prefix = Prompt("Path prefix inside bucket", "lancedb-sample-db");

            var options = new Dictionary<string, string>
            {
                { "access_key_id", accessKeyId },
                { "access_key_secret", accessKeySecret },
                { "endpoint", endpoint },
            };
            return ($"oss://{bucket}/{prefix}", options, false);
        }

        private static (string uri, Dictionary<string, string> options, bool readOnly) ConfigureHuggingFace()
        {
            string repo = Prompt("HF dataset repo (owner/name)", "");
            string token = PromptSecret("HF token (leave blank for public repos)");

            var options = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(token))
            {
                options["hf_token"] = token;
            }
            return ($"hf://datasets/{repo}", options, true);
        }

        private static void Assert(bool condition, string message)
        {
            if (!condition)
            {
                throw new Exception($"Assertion failed: {message}");
            }
        }

        private static string Prompt(string label, string defaultValue)
        {
            string suffix = string.IsNullOrEmpty(defaultValue) ? "" : $" [{defaultValue}]";
            Console.Write($"{label}{suffix}: ");
            string? input = Console.ReadLine();
            return string.IsNullOrWhiteSpace(input) ? defaultValue : input.Trim();
        }

        private static string PromptSecret(string label)
        {
            Console.Write($"{label}: ");
            var sb = new System.Text.StringBuilder();
            while (true)
            {
                var keyInfo = Console.ReadKey(intercept: true);
                if (keyInfo.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine();
                    break;
                }
                if (keyInfo.Key == ConsoleKey.Backspace)
                {
                    if (sb.Length > 0)
                    {
                        sb.Length--;
                        Console.Write("\b \b");
                    }
                    continue;
                }
                if (!char.IsControl(keyInfo.KeyChar))
                {
                    sb.Append(keyInfo.KeyChar);
                    Console.Write('*');
                }
            }
            return sb.ToString();
        }
    }
}
