-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE BlobOffsets (
	Id int identity PRIMARY KEY,
	Uri nvarchar(850) UNIQUE NOT NULL,
	Offset bigint NOT NULL
)
GO