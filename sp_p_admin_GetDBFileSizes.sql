USE [rp_util]
GO

IF OBJECT_ID('dbo.p_admin_GetDBFileSizes','P') IS NOT NULL
	DROP PROC dbo.p_admin_GetDBFileSizes;
GO

CREATE PROC dbo.p_admin_GetDBFileSizes
	@IsDisplay BIT = 0
AS
/*******************************************************************************
DESCRIPTION:
	captures current file sizes, free space and VLF counts for all dbs

PARAMETERS:
	@IsDisplay - show result or write to table
					1 - show result
					0 - write to table (default)

USAGE:
	exec [dbo].[p_admin_GetDBFileSizes]
		@IsDisplay = 1

HISTORY:
	03012011 - neh - sp created
	09292011 - neh - added FileName, Growth and BakFileName
					- removed VLFAvgSize_MB
	03012012 - neh - reworked DATABASE FILE SIZE and BAK FILE INFO logic
					- added @IsDisplay input parameter
	03012016 - neh - PRN 14099 - SQL Server 2012 upgrade: DBCC LOGINFO added a field: RecoveryUnitId
	
***************************************************************************************/
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET QUOTED_IDENTIFIER ON;

DECLARE @SQL NVARCHAR(MAX);

CREATE TABLE #DBFiles (
	DatabaseId INT
	, DatabaseName NVARCHAR(128)
	, FileGroupId INT
	, FileGroupName NVARCHAR(128)
	, [FileName] NVARCHAR(128)
	, [Size] BIGINT
	, Used BIGINT
	, Growth BIGINT
	, [type] TINYINT
	, FileId INT
	, AsOfDate DATETIME
);

CREATE TABLE #BakInfo (
	DatabaseId INT
	, BackupDevice NVARCHAR(128)
	, [Size] INT
);

CREATE TABLE #LogInfo (
	DatabaseName NVARCHAR(128)
	, RecoveryUnitId INT
	, FileId INT
	, FileSize BIGINT
	, StartOffset BIGINT
	, FSeqNo INT
	, [Status] TINYINT
	, Parity TINYINT
	, CreateLSN NUMERIC(25,0)
);

/***** START: GET DATABASE FILE SIZE AND PCT FREE SPACE *****/
SET @SQL = 
'USE [?];
INSERT INTO #DBFiles (
	DatabaseId
	, DatabaseName
	, FileGroupId
	, FileGroupName
	, [FileName]
	, [Size]
	, Used
	, Growth
	, [type]
	, FileId
	, AsOfDate
)
SELECT	DB_ID()
		, DB_NAME()
		, FileGroupId = f.data_space_id
		, FileGroupName = CASE WHEN f.data_space_id = 0 THEN ''LOG''
								ELSE s.name
							END
		, [FileName] = f.physical_name 
		, [Size] = CONVERT(BIGINT, f.size) * 8 / 1024 -- MB
		, Used = CONVERT(BIGINT, FILEPROPERTY(f.name, ''SpaceUsed'')) * 8 / 1024 -- MB
		, [Growth] = CASE f.is_percent_growth WHEN 1 THEN CONVERT(NVARCHAR(15), f.growth) --+ N''%''
												ELSE CONVERT(NVARCHAR(15), CONVERT(BIGINT, growth) * 8 / 1024) -- MB
						END
		, [type] = f.[type]
		, FileId = f.[file_id]
		, GETDATE()
FROM	sys.database_files f
		LEFT OUTER JOIN sys.data_spaces s
			ON f.data_space_id = s.data_space_id';

EXEC sp_msforeachdb @SQL;
/***** END: GET DATABASE FILE SIZE AND PCT FREE SPACE *****/


/***** START: GET BAK FILE INFO *****/
WITH BakSetDrvr (DBName, MaxBakDate)
AS (
	SELECT	RTRIM(database_name)
			, MAX(backup_start_date)
	FROM	msdb..backupset
	GROUP BY database_name
)

INSERT INTO #BakInfo (
	DatabaseId
	, BackupDevice
	, [Size]
)
SELECT  DatabaseId = d.database_id
		, BackupDevice = ISNULL(bmf.logical_device_name, bmf.physical_device_name)
        , [Size] = CONVERT(INT, bs.compressed_backup_size / 1024 /*KB*/ / 1024 /*MB*/)
FROM    sys.databases d
		LEFT OUTER JOIN BakSetDrvr tmp ON RTRIM(d.[name]) = tmp.DBName
        LEFT OUTER JOIN msdb..backupset bs
			ON RTRIM(bs.database_name) = tmp.DBName
			AND bs.backup_start_date = tmp.MaxBakDate
        LEFT OUTER JOIN msdb..backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
WHERE   d.[name] <> 'tempdb'
/***** END: GET BAK FILE INFO *****/


/***** START: GET LOG INFO *****/
SET @SQL = 
'USE [?];
INSERT INTO #LogInfo (
	RecoveryUnitId
	, FileId
	, FileSize
	, StartOffset
	, FSeqNo
	, Status
	, Parity
	, CreateLSN
)
EXEC(''DBCC LOGINFO'')

UPDATE	#LogInfo
SET		DatabaseName = DB_NAME()
WHERE	DatabaseName IS NULL';

EXEC sp_msforeachdb @SQL;
/***** END: GET LOG INFO *****/


IF @IsDisplay = 1
	SELECT	f.DatabaseName
			, Filegroup = f.FileGroupName
			, f.[FileName]
			, Size_MB = f.[Size]
			, Free_MB = f.[Size] - f.Used
			, PctFree = ((f.[Size] - f.Used) * 1.0 / NULLIF(f.[Size],0))
			, f.AsOfDate
			, VLFs = l.Qty
			, f.Growth
			, BakFileName = b.BackupDevice
			, BakSize = b.Size
	FROM	#DBFiles f
			LEFT OUTER JOIN #BakInfo b
				ON f.DatabaseId = b.DatabaseId
			LEFT OUTER JOIN (
				SELECT	DatabaseName
						, COUNT(FileId) AS Qty
				FROM	#LogInfo
				GROUP BY DatabaseName
			) l ON f.DatabaseName = l.DatabaseName
	ORDER BY f.DatabaseName
			, f.[type]
			, f.FileId;
ELSE
BEGIN
	IF NOT EXISTS (
		SELECT	1
		FROM	sys.objects
		WHERE	[name] = 'DBFileSizes'
		AND		[type] = 'U'
	)
	BEGIN
		CREATE TABLE [dbo].[DBFileSizes] (
			[DatabaseName] NVARCHAR(128) NOT NULL,
			[Filegroup] NVARCHAR(128) NOT NULL,
			[Size_MB] INT NOT NULL,
			[Free_MB] INT NOT NULL,
			[PctFree] NUMERIC(5,4) NOT NULL,
			[AsOfDate] DATETIME NOT NULL,
			[VLFs] INT NULL,
			[FileName] NVARCHAR(128) NULL,
			[Growth] INT NULL,
			[BakFileName] NVARCHAR(128) NULL,
			[BakSize] INT NULL,
			Uploaded BIT NOT NULL CONSTRAINT df_DBFileSizes_Uploaded DEFAULT 0
		);
		
		ALTER TABLE [dbo].[DBFileSizes] ADD  CONSTRAINT [PK_DBFileSizes] UNIQUE CLUSTERED 
		(
			[DatabaseName] ASC,
			[Filegroup] ASC,
			[FileName] ASC,
			[AsOfDate] ASC
		)WITH (FILLFACTOR = 95)
	END;


	INSERT INTO dbo.DBFileSizes (
		DatabaseName
		, Filegroup
		, [FileName]
		, Size_MB
		, Free_MB
		, PctFree
		, AsOfDate
		, VLFs
		, Growth
		, BakFileName
		, BakSize
	)
	SELECT	f.DatabaseName
			, f.FileGroupName
			, f.[FileName]
			, f.[Size]
			, f.[Size] - f.Used
			, ((f.[Size] - f.Used) * 1.0 / NULLIF(f.[Size],0))
			, f.AsOfDate
			, l.Qty
			, f.Growth
			, b.BackupDevice
			, b.Size
	FROM	#DBFiles f
			LEFT OUTER JOIN #BakInfo b
				ON f.DatabaseId = b.DatabaseId
			LEFT OUTER JOIN (
				SELECT	DatabaseName
						, COUNT(FileId) AS Qty
				FROM	#LogInfo
				GROUP BY DatabaseName
			) l ON f.DatabaseName = l.DatabaseName;
END;


GO


