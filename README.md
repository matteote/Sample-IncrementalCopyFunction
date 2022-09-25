# Sample-IncrementalCopyFunction

This repository contains a sample Azure Function that demonstrates how to perform the incremental copy of Azure Storage blobs.

The function expects the source blobs to be block blobs populated in an append-only fashion (data are added to the end of the file).

The newly appended data are copied to a target blob, in a separate container.

## Content

### `Sample-IncrementalCopyFunction` folder

The C# project of the Azure Function.

### `BlockBlobClientRangeCopyExtension` folder

The C# project of an extension to BlockBlobClient that implements a new `CopyRangeFromUri` method to perform copy of arbitrary ranges (virtually of any size) between block blobs.

## Prerequisites

### Tools

To build the sample code you need [.NET 6.0 SDK](https://learn.microsoft.com/en-us/dotnet/core/install/).

The deployment can be performed in multiple ways; the required tools vary based on the method used. For more information about the available options, see [Deployment technologies in Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies).

### Azure resources

#### Azure Function

The Function can be [tested locally](https://learn.microsoft.com/en-us/azure/azure-functions/functions-develop-local), or deployed to Azure.

For the deployment to Azure, the function requires a Function App resource. For an introduction to the creation of a Function App, refer to [Create your first function in the Azure portal](https://learn.microsoft.com/en-us/azure/azure-functions/functions-create-function-app-portal). For an overview of the available deployment technologies, see [Deployment technologies in Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies).

The Function uses a managed identity to access Azure Storage and (optionally) Azure SQL Database. You must assign an identity as described in [How to use managed identities for App Service and Azure Functions](https://learn.microsoft.com/en-us/azure/app-service/overview-managed-identity).

#### Azure SQL Database

The sample code stores information in an Azure SQL Database. You must [provision a database](https://learn.microsoft.com/en-us/azure/azure-sql/database/single-database-create-quickstart) and execute the `BlobOffsets.sql` file to generate the required SQL objects. The script can be executed with any SQL Server client including (but not limited to) [Azure Data Studio](https://learn.microsoft.com/en-us/sql/azure-data-studio/quickstart-sql-database), [SQL Server Management Studio](https://learn.microsoft.com/en-us/azure/azure-sql/database/connect-query-ssms), [the Azure Portal query editor](https://learn.microsoft.com/en-us/azure/azure-sql/database/connect-query-portal) and [Visual Studio Code](https://learn.microsoft.com/en-us/azure/azure-sql/database/connect-query-vscode).

Alternatively, you can run the sample code locally, replacing Azure SQL Database with a SQL Server instance or the [Azure SQL Database emulator](https://learn.microsoft.com/en-us/azure/azure-sql/database/local-dev-experience-sql-database-emulator).

If the Function must access the database using a managed identity, you need to grant access to the identity inside the database. To setup Azure AD authentication in Azure SQL Database see [Configure and manage Azure AD authentication with Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure).

The user used to access the database must have read and write permissions on the `BlobOffsets` table. For test purposes, this can be achieved by assigning it the database roles `db_datareader` and `db_datawriter`.

#### Azure Storage

The function copies data from arbitrary blobs to a specified storage account and container. You must provision the [target storage account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create) and [container](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container).

To test basic copy functionality, you can create a container and upload files to be used as a source. You can follow the basic steps described in [Quickstart: Upload, download, and list blobs with the Azure portal](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal).

To test the sample code locally, you can alternatively use the [Azurite emulator](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite).

The managed identity of the Function App must be assigned at least the `Storage Blob Data Reader` on the storage account(s) containing the source blobs, and the `Storage Blob Data Contributor` on the target storage account.

## Configuration

The Function has the following settings.

| Setting                | Description                                               |
|------------------------|-----------------------------------------------------------|
| SCHEDULE               | The schedule of the Function in NCRONTAB format. See [NCRONTAB expressions](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=in-process&pivots=programming-language-csharp#ncrontab-expressions).                            |
| SQL_CONNECTION_STRING  | The connection string of the database.                    |
| SQL_USE_CREDENTIAL     | (true/false) Use an AAD token to connect to the database. |
| TARGET_STORAGE_ACCOUNT | Name of the target storage account (name only, no         |
| TARGET_CONTAINER       | Name of the target container                              |

After deploying the Function, you must configure the all the settings above.

## Usage

Once the Function is built, deployed and configured, you can add files to be copied to
the `BlobOffsets` table with a query like this:

```SQL
INSERT INTO BlobOffsets(Uri, Offset)
VALUES ('<URI of the source blob>', 0)
```

Example:

```SQL
INSERT INTO BlobOffsets(Uri, Offset)
VALUES ('https://myaccount.blob.core.windows.net/mycontainer/folder1/file1.csv', 0)
```

## More details

During each execution, the function checks all the blobs contained in the `BlobOffsets` table.

It compares the offset stored in the table with the current size of the blob. The offset represents the point inside the source blob that has been copied so far.

If the current size of the blob is larger than the offset stored in the database, the Function copies the new portion of the file (between the previous offset and the end of the file) to a new blob in the target container.

The newly generated file uses the same name, plus a timestamp.

Using the example above, the copy of data from the blob

```https://myaccount.blob.core.windows.net/mycontainer/folder1/file1.csv```

generates a new blob with a URI like

```https://<TARGET_STORAGE_ACCOUNT>.blob.core.windows.net/<TARGET_CONTAINER>/folder1/file1.csv_<TIMESTAMP>```

The newly generated blob contains only the latest portion of the source blob.

