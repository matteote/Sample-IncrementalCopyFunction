// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using BlockBlobClientCopyRangeExtension;
using Microsoft.Azure.WebJobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace IncrementalCopyFunction
{
    public class IncrementalCopyFunction
    {
        [FunctionName("IncrementalCopyFunction")]
        public async Task Run([TimerTrigger("%SCHEDULE%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation("Function starting.");

            // Process all blobs tracked in the database
            await foreach ((Uri sourceBlobUri, long currentOffset) in GetTrackedBlobs())
            {
                log.LogInformation("Checking blob: {uri}", sourceBlobUri.ToString());

                var sourceBlockBlobClient = GetBlockBlobClient(sourceBlobUri);

                // Check if the blob exists
                if (await sourceBlockBlobClient.ExistsAsync())
                {
                    var blobProperties = (await sourceBlockBlobClient.GetPropertiesAsync()).Value;

                    // Check if the blob size grew
                    if (blobProperties.ContentLength > currentOffset)
                    {
                        log.LogInformation(
                            "Processing changes in blob: {uri}",
                            sourceBlobUri.ToString());

                        var copyLength = blobProperties.ContentLength - currentOffset;
                        var targetBlobUri = GetTargetBlobUri(sourceBlobUri);

                        var targetBlockBlobClient = GetBlockBlobClient(targetBlobUri);

                        // Copy the newest portion of the source blob to the target blob
                        await targetBlockBlobClient.CopyRangeFromUriAsync(
                            sourceBlobUri,
                            currentOffset,
                            copyLength);

                        // Update the database
                        await UpdateBlobOffset(sourceBlobUri, blobProperties.ContentLength);

                        log.LogInformation(
                            "Copied {length} bytes from {sourceBlobUri} to {targetBlobUri}",
                            copyLength,
                            sourceBlobUri.ToString(),
                            targetBlobUri.ToString());
                    }
                    else
                    {
                        log.LogInformation(
                            "No change in blob: {uri}",
                            sourceBlobUri.ToString());
                    }
                }
                else
                {
                    log.LogWarning("Blob not found: {uri}", sourceBlobUri.ToString());
                }
            }

            log.LogInformation("Function completed.");
        }

        // Instantiates a new BlockBlobClient for the specified URI
        private BlockBlobClient GetBlockBlobClient(Uri uri)
        {
            var credential = new DefaultAzureCredential();

            return new BlockBlobClient(
                    uri,
                    credential);
        }

        // Enumerates the blobs currently tracked in the database, returning
        // their URI and most recent offset.
        private async IAsyncEnumerable<(Uri uri, long offset)> GetTrackedBlobs()
        {
            var conn = await GetSqlConnection();

            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT Uri, Offset FROM BlobOffsets;";

            var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                yield return new(
                    new Uri(reader.GetString(0)),
                    reader.GetSqlInt64(1).Value);
            }

            await reader.CloseAsync();
            await conn.CloseAsync();
        }

        // Returns the URI of the blob that data should be copied to, based
        // on the URI of the source blob.
        // The name preserves the original blob name (including folders), adding a timestamp.
        private Uri GetTargetBlobUri(Uri uri)
        {
            var targetStorageAccount = Environment.GetEnvironmentVariable("TARGET_STORAGE_ACCOUNT");
            var targetContainer = Environment.GetEnvironmentVariable("TARGET_CONTAINER");

            BlobUriBuilder blobUriBuilder = new(uri);

            blobUriBuilder.Host = $"{targetStorageAccount}.blob.core.windows.net";
            blobUriBuilder.BlobContainerName = targetContainer;

            // A timestamp is appended to the blob name
            blobUriBuilder.BlobName += "_" + DateTime.UtcNow.ToString("yyyyMMddHHmmss");

            return blobUriBuilder.ToUri();
        }

        // Updates the offset of the specified blob.
        private async Task UpdateBlobOffset(Uri uri, long offset)
        {
            var conn = await GetSqlConnection();

            var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE BlobOffsets SET Offset=@Offset WHERE Uri=@Uri";
            cmd.Parameters.Add("@Offset", System.Data.SqlDbType.BigInt)
                .Value = offset;
            cmd.Parameters.Add("@Uri", System.Data.SqlDbType.NVarChar)
                .Value = uri.ToString();

            await cmd.ExecuteNonQueryAsync();

            await conn.CloseAsync();
        }

        // Creates and open a SqlConnection to the database containing
        // blob offset information.
        private async Task<SqlConnection> GetSqlConnection()
        {
            var connstring = Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING");

            var conn = new SqlConnection(connstring);

            if (bool.TryParse(
                Environment.GetEnvironmentVariable("SQL_USE_CREDENTIAL"),
                out var useCredential))
            {
                if (useCredential)
                {
                    var credential = new DefaultAzureCredential();
                    var token = credential.GetToken(
                        new Azure.Core.TokenRequestContext(
                            new[] { "https://database.windows.net/.default" }));
                    conn.AccessToken = token.Token;
                }
            }

            await conn.OpenAsync();

            return conn;
        }
    }
}
