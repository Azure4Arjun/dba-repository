USE rp_util;
GO

/*
IF EXISTS(SELECT 1 FROM sys.tables WHERE [name] = 'T_UTIL_IndxDefragLog')
    DROP TABLE T_UTIL_IndxDefragLog
IF EXISTS(SELECT 1 FROM sys.tables WHERE [name] = 'T_UTIL_IndxDefragStatus')
    DROP TABLE T_UTIL_IndxDefragStatus
Go

CREATE TABLE dbo.T_UTIL_IndxDefragLog (
	IndxDefragId INT IDENTITY(1,1) NOT NULL
	, [DBId] INT NOT NULL
    , DBName NVARCHAR(128) NOT NULL
    , TblId INT NOT NULL
    , TblName NVARCHAR(128) NOT NULL
    , IndxId INT NOT NULL
    , IndxName NVARCHAR(128) NOT NULL
    , PartitionNumber INT NOT NULL
    , Fragmentation FLOAT NOT NULL
    , [PageCount] INT NOT NULL
    , DateTimeStart DATETIME NOT NULL
    , DateTimeEnd DATETIME NULL
    , DurationSecs AS CONVERT(INT,DATEDIFF(ss,DateTimeStart,DateTimeEnd))
    , SQLStmt VARCHAR(4000) NULL
    , ErrMsg VARCHAR(1000) NULL
	, CONSTRAINT PK_T_UTIL_IndxDefragLog PRIMARY KEY CLUSTERED (IndxDefragId)
)
GO

CREATE TABLE dbo.T_UTIL_IndxDefragStatus (
	[DBId] INT NOT NULL
    , DBName NVARCHAR(128) NOT NULL
	, SchemaName NVARCHAR(128) NULL
	, TblId INT NOT NULL
	, TblName NVARCHAR(128) NULL
	, IndxId INT NOT NULL
	, IndxName NVARCHAR(128) NULL
    , PartitionNumber INT NOT NULL
    , Fragmentation FLOAT NULL
    , [PageCount] INT NULL
	, RangeScanCount BIGINT NULL
	, ScanDate DATETIME NULL
	, DefragDate DATETIME NULL
	, PrintStatus BIT CONSTRAINT df_T_UTIL_IndxDefragStatus_PrintStatus DEFAULT(0)
	, CONSTRAINT PK_T_UTIL_IndxDefragStatus PRIMARY KEY CLUSTERED ([DBId], TblId, IndxId)
)
GO
*/

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = object_id(N'[dbo].[p_admin_reindex_rp3]') AND OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[p_admin_reindex_rp3];
GO


CREATE PROC dbo.p_admin_reindex_rp3
	@DBName NVARCHAR(128) = NULL
	, @TblId INT = NULL
	, @AllIndxs BIT = 1
	, @MinFragmentation REAL = 5.0 -- in percent
	, @RebuildThreshold REAL = 30.0 -- in percent
	, @ExecSQL BIT = 1
    , @DefragOrderColumn NVARCHAR(20) = 'RANGESCANCOUNT'
    , @DefragSortOrder NVARCHAR(4) = 'DESC'
    , @TimeLimit INT = 720
    , @ForceRescan BIT = 0
    , @ScanMode VARCHAR(10) = N'LIMITED'
    , @MinPageCount INT = 1000
    , @MaxPageCount INT = NULL
    , @OnlineRebuild BIT = 1
    , @SortInTempDB BIT = 1
    , @MaxDopRestriction TINYINT = NULL
    , @PrintCommands BIT = 0
    , @PrintFragmentation BIT = 0
    , @DebugMode BIT = 0
    , @UpdStaleStatistics BIT = 0
	, @LOBCompaction BIT = 0
AS
/*******************************************************************************
DESCRIPTION:
	re-index & update statistics for user tables for specified db

PARAMETERS:
	@DBName				- Option to specify a database name; null will return all
	@TblId				- Option to specify a table id; null will return all
	@AllIndxs			- 1 = all indexes
						- 0 = clustered indexes ONLY
	@MinFragmentation	- Defaulted to 5% as recommended by MS in BOL; will not defrag if fragmentation is less than specified
	@RebuildThreshold	- Defaulted to 30% as recommended by MS in BOL; greater than 30% will result in rebuild instead or reorg
	@ExecSQL			- 1 = execute
						- 0 = print command only
    @DefragOrderColumn	- Defines how to prioritize the order of defrags.  Only used if @ExecSQL = 1.  
						- Valid options are: 
							'RANGESCANCOUNT' = count of range and table scans on the index; in general, this is what benefits
								the most from defragmentation
							'FRAGMENTATION' = amount of fragmentation in the index; the higher the number, the worse it is
							'PAGECOUNT' = number of pages in the index; affects how long it takes to defrag an index
    @DefragSortOrder	- The sort order of the ORDER BY clause. Valid options are ASC (ascending) or DESC (descending).
    @TimeLimit			- Defaulted to 12 hours - optional limit to how much time can be spent performing index defrags; expressed in minutes.
							NOTE: The time limit is checked BEFORE an index defrag is begun, thus a long index defrag can exceed the
                                  time limitation.
    @ForceRescan		- Whether or not to force a rescan of indexes.  If set to 0, a rescan will not occur until all indexes have
							been defragged.  This can span multiple executions.
							1 = force a rescan
							0 = use previous scan, if there are indexes left to defrag
    @ScanMode			- Specifies which scan mode to use to determine fragmentation levels.  Options are:
                            'LIMITED' - scans the parent level; quickest mode, recommended for most cases.
                            'SAMPLED' - samples 1% of all data pages; if less than 10k pages, performs a DETAILED scan.
                            'DETAILED' - scans all data pages.  Use great care with this mode, as it can cause performance issues.
    @MinPageCount		- Specifies how many pages must exist in an index in order to be considered for a defrag. Defaulted to 1000 pages as 
                            recommended by MS.  
							NOTE: The @MinPageCount will restrict the indexes that are stored in T_UTIL_IndxDefragStatus table.
    @MaxPageCount		- Specifies the maximum number of pages that can exist in an index and still be considered for a defrag.  Useful
							for scheduling small indexes during business hours and large indexes for non-business hours.
							NOTE: The @maxPageCount will restrict the indexes that are defragged during the current operation; it will not
							prevent indexes from being stored in the T_UTIL_IndxDefragStatus table.  This way, a single scan can support
							multiple page count thresholds.
    @OnlineRebuild		- 1 = online rebuild
						- 0 = offline rebuild; only in Enterprise
    @SortInTempDB		- Specifies whether to defrag the index in TEMPDB or in the database the index belongs to.  Enabling this option may
							result in faster defrags and prevent database file size inflation.
							1 = perform sort operation in TempDB
							0 = perform sort operation in the index's database 
    @MaxDopRestriction	- Option to specify a processor limit for index rebuilds; only in Enterprise
    @PrintCommands		- 1 = print commands to screen
						- 0 = do not print commands
    @PrintFragmentation	- 1 = print fragmentation to screen
						- 0 = do not print fragmentation
    @DebugMode			- 1 = display debug comments; helps with troubleshooting
						- 0 = do not display debug comments
	@UpdStaleStatistics	- 1 = check for and update stale statistics
						- 0 = omit stale statistics handling
	@LOBCompaction		- 1 = compact pages that contain LOB columns when reorganizing indexes
						- 0 = do not compact pages that contain LOB columns when reorganizing indexes


USAGE:
	exec dbo.p_admin_reindex_rp3
		@DBName = 'rp_prod'
		,@SortInTempDB = 1;

HISTORY:
	05202003 - neh - sp created
	09252003 - neh - dynamic and streamlined
	10202003 - neh - added fillfactor parameter to use existing index fillfactor
	12222003 - neh - modified to reindex once a day per table
			 modified to use INDEXDEFRAG instead of DBREINDEX to reduce table locking
	12232003 - neh - added criteria for tables to be checked for reindexing (no "old" or "temp")
			 expanded to reindex at an index rather than at a table level
	06092004 - neh - modified file truncation
			 made dynamic so it can be dropped and ran against any db
	03212006 - neh - added database specific logic
	04052006 - neh - modified to handle table names that contain spaces
	04062006 - neh - removed truncate file logic
	07142006 - neh - added non-clustered index re-indexing
			 modified logging to catch tables still fragmented after process
	06262008 - neh - merged update statistics loop into showcontig loop
			- modified defrag criteria
			- modified to use dbcc dbreindex (rebuilds) instead of dbcc indexdefrag (reorders)
			- removed nonclustered index defrag
			- removed hard-coded tables
	09152008 - neh - added @l_TblName and @all_indxs input parameters
			- added non-clustered index defrag
	02052009 - neh - added ScanDensity check to ExtentSwitches check
	09152009 - neh - added FileGroup, test_ScanDensity and Rows columns to action table
					- removed ExtentSwitching columns
					- added logic to skip heap tables
	02022010 - neh - modified to implement sql server 2005 changes
	02142011 - neh - implemented/modified Michelle Ufford's Index Defrag Script logic (http://sqlfool.com/scripts/dba_indexDefrag_sp_v40_public.txt)
	12212011 - neh - replaced @TblName parameter w/ @TblId
	12102012 - neh - replace TRUNCATE with DELETE
	02052013 - neh - added @UpdStaleStatistics input parameter to check for/update stale statistics
	07142016 - neh - modified defaults
					- added @LOBCompaction

***************************************************************************************/
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET QUOTED_IDENTIFIER ON;

-- declare variables
DECLARE	@l_DBId INT
		, @l_TblId INT
		, @l_TblName NVARCHAR(256)
        , @l_IndxId INT
        , @l_SchemaName NVARCHAR(128)
        , @l_IndxName NVARCHAR(128)
        , @l_Fragmentation FLOAT
        , @l_PageCount INT
        , @l_SQLCommand NVARCHAR(4000)
        , @l_RebuildCommand NVARCHAR(4000)
        , @l_TimeStart DATETIME
        , @l_TimeEnd DATETIME
        , @l_LOBCheck BIT
        , @l_EditionCheck BIT
		, @l_DebugMsg NVARCHAR(4000)
        , @l_UpdSQL NVARCHAR(4000)
        , @l_LOBSQL NVARCHAR(4000)
        , @l_LOBSQLParam NVARCHAR(4000)
        , @l_IndxDefragId INT
        , @l_StartTime DATETIME
        , @l_EndTime DATETIME
        , @l_IndxSQL NVARCHAR(4000)
        , @l_IndxSQLParam NVARCHAR(4000)
        , @l_AllowPageLockSQL NVARCHAR(4000)
        , @l_AllowPageLockSQLParam NVARCHAR(4000)
        , @l_AllowPageLocks BIT
        , @l_StatisticsSQL NVARCHAR(4000)
        , @l_RowId INT
        , @l_RecompileSQL NVARCHAR(4000)
        , @l_DefragChk BIT
        , @l_UpdStaleStatisticsSQL NVARCHAR(4000)
        , @l_UpdStatisticsSQL NVARCHAR(4000)
        , @l_PartitionNumber INT
		, @l_MaxDOP INT;

-- declare temp tables
DECLARE @t_DBList TABLE (
	[DBId] INT
	, DBName NVARCHAR(128)
	, ScanStatus BIT
);
		
DECLARE @t_RecompileList TABLE (
	RowId INT IDENTITY(1,1)
	, DBName NVARCHAR(128)
	, SchemaName NVARCHAR(128)
	, TblName NVARCHAR(128)
	, Processed BIT
);

BEGIN TRY;
	-- input parameter validation/default values
	IF @AllIndxs IS NULL
		SET @AllIndxs = 1;

	IF @MinFragmentation IS NULL
	OR @MinFragmentation NOT BETWEEN 0.00 AND 100.0
		SET @minFragmentation = 5.0;

	IF @RebuildThreshold IS NULL
	OR @RebuildThreshold NOT BETWEEN 0.00 AND 100.0
		SET @RebuildThreshold = 30.0;

	IF @ExecSQL IS NULL
		SET @ExecSQL = 1;

	IF @DefragOrderColumn IS NULL
	OR @DefragOrderColumn NOT IN ('RANGESCANCOUNT', 'FRAGMENTATION', 'PAGE_COUNT')
		SET @DefragOrderColumn = 'RANGESCANCOUNT';

    IF @DefragSortOrder IS NULL
	OR @DefragSortOrder NOT IN ('ASC', 'DESC')
		SET @DefragSortOrder = 'DESC';

	IF @ForceRescan IS NULL
		SET @ForceRescan = 0;

	IF @ScanMode NOT IN ('LIMITED', 'SAMPLED', 'DETAILED')
		SET @ScanMode = 'LIMITED';

	IF @MinPageCount IS NULL
		SET @MinPageCount = 1000;

	IF @OnlineRebuild IS NULL
		SET @OnlineRebuild = 1;

	IF @SortInTempDB IS NULL
		SET @SortInTempDB = 1;

	IF @PrintCommands IS NULL
		SET @PrintCommands = 0;

	IF @PrintFragmentation IS NULL
		SET @PrintFragmentation = 0;

	IF @DebugMode IS NULL
		SET @DebugMode = 0;

	IF @UpdStaleStatistics IS NULL
		SET @UpdStaleStatistics = 0;

	IF @LOBCompaction IS NULL
		SET @LOBCompaction = 0;
		
	IF @DebugMode = 1
		RAISERROR('Reindexing functionality starting up...', 0, 42) WITH NOWAIT;

	-- initialize variables
	SET @l_StartTime = GETDATE();
	SET @l_EndTime = DATEADD(MI, @TimeLimit, @l_StartTime);

	IF @DebugMode = 1
		RAISERROR('Beginning validation...', 0, 42) WITH NOWAIT;

	-- find MAXDOP
	SELECT	@l_MaxDOP = CAST(value_in_use AS INT)
	FROM	sys.configurations
	WHERE	name = 'max degree of parallelism';

	-- if MaxDOP specified and doesn't exceed number of processors available use specified value
	IF @MaxDopRestriction IS NOT NULL
	AND	@MaxDopRestriction < @l_MaxDOP
		SET @l_MaxDOP = @MaxDopRestriction;

	-- check server version supports online rebuilds
	-- 1804890536 = Enterprise, 1872460670 = Enterprise Edition: Core-based Licensing, 610778273 = Enterprise Evaluation, -2117995310 = Developer
    IF (SELECT SERVERPROPERTY('EditionID')) IN (1804890536, 1872460670, 610778273, -2117995310)
        SET @l_EditionCheck = 1; -- supports online rebuilds
    ELSE
        SET @l_EditionCheck = 0; -- does not support online rebuilds

    IF @DebugMode = 1
		RAISERROR('Grabbing db list...', 0, 42) WITH NOWAIT;

	-- retrieve db list to investigate
	IF @DBName IS NULL
		INSERT INTO @t_DBList ([DBId], DBName, ScanStatus)
		SELECT	database_id
				, name
				, 0 -- not scanned for fragmentation
		FROM	sys.databases
		WHERE	[name] != 'tempdb' -- exclude tempdb
		AND		[state] = 0 -- state must be ONLINE
		AND		is_read_only = 0; -- cannot be READ_ONLY
	ELSE
		INSERT INTO @t_DBList ([DBId], DBName, ScanStatus)
		SELECT	DB_ID(@DBName)
				, @DBName
				, 0; -- not scanned for fragmentation

	-- check for and update stale statistics
	IF @UpdStaleStatistics = 1
	BEGIN
        IF @DebugMode = 1
			RAISERROR('Updating out of date statistics...', 0, 42) WITH NOWAIT;

		CREATE TABLE #UpdStaleStats (
			RowId INT IDENTITY(1,1) PRIMARY KEY CLUSTERED
			, UpdateStmt NVARCHAR(1000)
			, Processed BIT DEFAULT 0
		);
		
		SET @l_UpdStaleStatisticsSQL =
			CASE WHEN @DBName IS NULL THEN 'USE [?];'
				ELSE 'USE ' + @DBName + ';'
			END + '
			INSERT INTO #UpdStaleStats (UpdateStmt)
			SELECT	DISTINCT
					'' UPDATE STATISTICS ['' + DB_NAME() + ''].['' + SCHEMA_NAME(o.schema_id) + ''].['' + o.name + ''] ['' + t.name + ''];''
			FROM	sys.stats t
					INNER JOIN sys.objects o ON o.object_id = t.object_id
					CROSS APPLY sys.dm_db_stats_properties(t.object_id, t.stats_id)
			WHERE	ISNULL(modification_counter,0) > 0
			OR		DATEDIFF(DAY,last_updated,GETDATE()) > 30';

		IF @DBName IS NULL
			EXEC sp_msforeachdb @l_UpdStaleStatisticsSQL;
		ELSE
			EXEC sp_executesql @statement = @l_UpdStaleStatisticsSQL;
		
		-- omit tempdb objects
		DELETE FROM #UpdStaleStats WHERE UpdateStmt LIKE '%tempdb%';

		WHILE EXISTS (SELECT 1 FROM #UpdStaleStats WHERE Processed = 0)
		BEGIN
			SELECT	TOP 1
					@l_RowId = RowId
					, @l_UpdStatisticsSQL = UpdateStmt
			FROM	#UpdStaleStats
			WHERE	Processed = 0
			ORDER BY RowId;
			
			EXEC sp_executesql @statement = @l_UpdStatisticsSQL;
		
			UPDATE	#UpdStaleStats
			SET		Processed = 1
			WHERE	RowId = @l_RowId;
		END;
	END;

	-- check if we have indxs in need of defrag; otherwise, re-scan the db(s)
    IF NOT EXISTS(SELECT 1 FROM dbo.T_UTIL_IndxDefragStatus WHERE DefragDate IS NULL)
	OR @ForceRescan = 1
    BEGIN
		-- truncate list of indexes to prep for new scan
        DELETE FROM dbo.T_UTIL_IndxDefragStatus;
        
        IF @DebugMode = 1
			RAISERROR('Looping thru list of dbs checking for fragmentation...', 0, 42) WITH NOWAIT;

		-- loop thru list of dbs
        WHILE EXISTS (SELECT 1 FROM @t_DBList WHERE ScanStatus = 0)
        BEGIN
            SELECT	TOP 1 @l_DBId = [DBId]
            FROM	@t_DBList
            WHERE	ScanStatus = 0;

            IF @DebugMode = 1
            BEGIN
				SELECT @l_DebugMsg = '  working on ' + DB_Name(@l_DBId) + '...';

				RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
			END;

			-- determine indexes to defrag using user-defined params
            INSERT INTO dbo.T_UTIL_IndxDefragStatus (
				[DBId]
				, DBName
				, TblId
				, IndxId
				, PartitionNumber
				, Fragmentation
				, [PageCount]
				, RangeScanCount
				, ScanDate
            )
            SELECT	ps.database_id
					, QUOTENAME(DB_NAME(ps.database_id))
					, ps.object_id
					, ps.index_id
					, ps.partition_number
					, SUM(ps.avg_fragmentation_in_percent)
					, SUM(ps.page_count)
					, os.range_scan_count
					, GETDATE()
            FROM	sys.dm_db_index_physical_stats(@l_DBId,@TblId,NULL,NULL,@ScanMode) AS ps
					INNER JOIN sys.dm_db_index_operational_stats(@l_DBId,@TblId,NULL,NULL) AS os
						ON ps.database_id = os.database_id
						AND ps.object_id = os.object_id
						AND ps.index_id = os.index_id
						AND ps.partition_number = os.partition_number
            WHERE	avg_fragmentation_in_percent > @MinFragmentation 
            AND		ps.index_id > 0 -- ignore heaps
            AND		ps.page_count >= @MinPageCount 
            AND		ps.index_level = 0 -- leaf-level nodes only, supports @ScanMode
			AND		ps.index_type_desc = CASE WHEN @AllIndxs = 0 THEN 'CLUSTERED INDEX' ELSE ps.index_type_desc END
            GROUP BY ps.database_id 
                , QUOTENAME(DB_NAME(ps.database_id))
                , ps.object_id
                , ps.index_id
				, ps.partition_number
                , os.range_scan_count
            OPTION (MAXDOP 1);

			-- mark scanned db(s)
            UPDATE	@t_DBList
            SET		ScanStatus = 1
            WHERE	[DBId] = @l_DBId;
		END;
	END;

	IF @DebugMode = 1
	BEGIN
		SELECT	@l_DebugMsg = 'There are ' + CAST(COUNT(*) AS VARCHAR(10)) + ' indxs to defrag...'
		FROM	dbo.T_UTIL_IndxDefragStatus
		WHERE	DefragDate IS NULL
		AND		[PageCount] BETWEEN @MinPageCount AND ISNULL(@MaxPageCount,[PageCount]);

		RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
	END;

	-- loop to defrag
	WHILE EXISTS (	SELECT	1
					FROM	dbo.T_UTIL_IndxDefragStatus 
					WHERE	((@ExecSQL = 1 AND DefragDate IS NULL) 
							OR (@ExecSQL = 0 AND DefragDate IS NULL AND PrintStatus = 0))
					AND		[PageCount] BETWEEN @MinPageCount AND ISNULL(@MaxPageCount,[PageCount]) )
	BEGIN
		-- check if we need to exit loop because of time limit
        IF ISNULL(@l_EndTime,GETDATE()) < GETDATE()
            RAISERROR('Time limit has been exceeded.', 11, 42) WITH NOWAIT;

        If @DebugMode = 1
			RAISERROR('  Picking an indx to defrag...', 0, 42) WITH NOWAIT;

		SET @l_DefragChk = NULL;
			
		-- find indx with highest priority, based on the values submitted
        SET @l_IndxSQL = N'SELECT	TOP 1
									@p_TblId_Out = TblId
									, @p_PartitionNumber_Out = PartitionNumber
									, @p_IndxId_Out = IndxId
									, @p_DBId_Out = [DBId]
									, @p_DBName_Out = DBName
									, @p_Fragmentation_Out = Fragmentation
									, @p_PageCount_Out = [PageCount]
						FROM	dbo.T_UTIL_IndxDefragStatus
						WHERE	DefragDate IS NULL ' 
						+ CASE WHEN @ExecSQL = 0 THEN 'AND PrintStatus = 0 ' ELSE '' END
						+ 'AND [PageCount] BETWEEN @p_MinPageCount AND ISNULL(@p_MaxPageCount,[PageCount])
						ORDER BY ' + @DefragOrderColumn + ' ' + @DefragSortOrder;

        SET @l_IndxSQLParam = N'@p_TblId_Out INT OUTPUT
							 , @p_PartitionNumber_Out INT OUTPUT
							 , @p_IndxId_Out INT OUTPUT
							 , @p_DBId_Out INT OUTPUT
							 , @p_DBName_Out NVARCHAR(128) OUTPUT
							 , @p_Fragmentation_Out INT OUTPUT
							 , @p_PageCount_Out INT OUTPUT
							 , @p_MinPageCount INT
							 , @p_MaxPageCount INT';

        EXEC sp_executesql
			@statement = @l_IndxSQL
            , @params = @l_IndxSQLParam
            , @p_MinPageCount = @MinPageCount
            , @p_MaxPageCount = @MaxPageCount
            , @p_TblId_Out = @l_TblId OUTPUT
            , @p_PartitionNumber_Out = @l_PartitionNumber OUTPUT
            , @p_IndxId_Out = @l_IndxId OUTPUT
            , @p_DBId_Out = @l_DBId OUTPUT
            , @p_DBName_Out = @DBName OUTPUT
            , @p_Fragmentation_Out = @l_Fragmentation OUTPUT
            , @p_PageCount_Out = @l_PageCount OUTPUT;

        IF @DebugMode = 1
			RAISERROR('  Finding specs for indx...', 0, 42) WITH NOWAIT;

		-- look up indx info
        SET @l_UpdSQL = N'UPDATE	ids
						Set	SchemaName = QUOTENAME(s.name)
							, TblName = QUOTENAME(o.name)
							, IndxName = QUOTENAME(i.name)
						FROM	dbo.T_UTIL_IndxDefragStatus ids
								INNER JOIN ' + @DBName + '.sys.objects o ON ids.TblId = o.object_id
								INNER JOIN ' + @DBName + '.sys.indexes i
									ON o.object_id = i.object_id
									AND ids.IndxId = i.index_id
								INNER JOIN ' + @DBName + '.sys.schemas s ON o.schema_id = s.schema_id
						Where	o.object_id = ' + CAST(@l_TblId AS VARCHAR(10)) + '
						AND		i.index_id = ' + CAST(@l_IndxId AS VARCHAR(10)) + '
						AND		i.type > 0
						AND		ids.[DBId] = ' + CAST(@l_DBId AS VARCHAR(10));

        EXEC sp_executesql
			@statement = @l_UpdSQL;

		-- get obj names
        SELECT	@l_TblName = TblName
				, @l_SchemaName = SchemaName
				, @l_IndxName = IndxName
        From	dbo.T_UTIL_IndxDefragStatus
        Where	TblId = @l_TblId
		And		IndxId = @l_IndxId
		And		[DBId] = @l_DBId;

        IF @DebugMode = 1
			RAISERROR('  Checking for LOBs...', 0, 42) WITH NOWAIT;

		-- check if tbl has any LOBs (Large Objects)
        SET @l_LOBSQL = 'SELECT @p_ContainsLOB_OUT = 1
						FROM ' + @DBName + '.sys.columns WITH (NOLOCK)
						WHERE object_id = ' + CAST(@l_TblId AS VARCHAR(10)) + '
						AND (system_type_id IN (34, 35, 99)
							OR max_length = -1)';
					-- system_type_id --> 34 = image, 35 = text, 99 = ntext
					-- max_length = -1 --> varbinary(max), varchar(max), nvarchar(max), xml

		SET @l_LOBSQLParam = '@p_ContainsLOB_OUT INT OUTPUT';

        EXEC sp_executesql
			@statement = @l_LOBSQL
			, @params = @l_LOBSQLParam
			, @p_ContainsLOB_OUT = @l_LOBCheck OUTPUT;

        IF @DebugMode = 1
			RAISERROR('  Checking whether indx allows page locks...', 0, 42) WITH NOWAIT;

		-- check if page locks are allowed; for those indexes, we need to always rebuild
        SET @l_AllowPageLockSQL = 'SELECT @p_AllowPageLocks_OUT = 1
									FROM ' + @DBName + '.sys.indexes
									WHERE object_id = ' + CAST(@l_TblId AS VARCHAR(10)) + '
									AND index_id = ' + CAST(@l_IndxId AS VARCHAR(10)) + '
									AND Allow_Page_Locks = 1';
								-- Allow_Page_Locks = 1 --> indx allows page locks

		SET @l_AllowPageLockSQLParam = '@p_AllowPageLocks_OUT INT OUTPUT';

        EXEC sp_executesql
			@statement = @l_AllowPageLockSQL
			, @params = @l_AllowPageLockSQLParam
			, @p_AllowPageLocks_OUT = @l_AllowPageLocks OUTPUT;

        IF @DebugMode = 1
			RAISERROR('  Building SQL stmt...', 0, 42) WITH NOWAIT;

		-- if there is not a lot of fragmentation or if we have a LOB then we should reorg
        IF (@l_Fragmentation < @RebuildThreshold
			OR ISNULL(@l_LOBCheck,0) = 1)
		--AND ISNULL(@l_AllowPageLocks,0) = 0
            SET @l_SQLCommand = N'ALTER INDEX ' + @l_IndxName
								+ N' ON ' + @DBName + N'.' + @l_SchemaName + N'.' + @l_TblName
								+ N' REORGANIZE'
								+ CASE WHEN @l_PartitionNumber > 1 THEN N' PARTITION = ' + CONVERT(VARCHAR,@l_PartitionNumber) ELSE N'' END;
		-- if the indx is heavily fragmented or does not allow page locks and doesn't contain any LOB's then rebuild it
		ELSE	IF (@l_Fragmentation >= @RebuildThreshold
					OR ISNULL(@l_AllowPageLocks,0) = 0)
				AND ISNULL(@l_LOBCheck,0) <> 1
		BEGIN
			-- set online rebuild options; requires Enterprise Edition
            IF @OnlineRebuild = 1
            AND @l_EditionCheck = 1 
                SET @l_RebuildCommand = N' REBUILD'
										+ CASE WHEN @l_PartitionNumber > 1 THEN N' PARTITION = ' + CONVERT(VARCHAR,@l_PartitionNumber) ELSE N'' END
										+ N' WITH (ONLINE = ON';
            ELSE
                SET @l_RebuildCommand = N' REBUILD'
										+ CASE WHEN @l_PartitionNumber > 1 THEN N' PARTITION = ' + CONVERT(VARCHAR,@l_PartitionNumber) ELSE N'' END
										+ N' WITH (ONLINE = OFF';

			-- set sort operation prefs
            IF @SortInTempDB = 1 
                SET @l_RebuildCommand = @l_RebuildCommand + N', SORT_IN_TEMPDB = ON';
            ELSE
                SET @l_RebuildCommand = @l_RebuildCommand + N', SORT_IN_TEMPDB = OFF';

			-- set processor restriction options; requires Enterprise Edition
            IF @MaxDopRestriction IS NOT NULL
            AND @l_EditionCheck = 1
                SET @l_RebuildCommand = @l_RebuildCommand + N', MAXDOP = ' + CAST(@MaxDopRestriction AS VARCHAR(2)) + N')';
            ELSE
                SET @l_RebuildCommand = @l_RebuildCommand + N')';

            SET @l_SQLCommand = N'ALTER INDEX ' + @l_IndxName
								+ N' ON ' + @DBName + N'.' + @l_SchemaName + N'.' + @l_TblName
								+ @l_RebuildCommand;
		END;
		-- raise error if indx does not meet the criteria above
        ELSE
        BEGIN
            IF @PrintCommands = 1
            OR @DebugMode = 1
            BEGIN
				SET @l_DebugMsg = 'Unable to defrag indx: ' + @l_IndxName;

                RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
			END;
			
			SET @l_DefragChk = 1;
			SET @l_SQLCommand = NULL;
		END;

		-- upd statistics stmt
		SET @l_StatisticsSQL = 'UPDATE STATISTICS ' + @DBName + N'.' + @l_SchemaName + N'.' + @l_TblName
								+ ' ' + @l_IndxName;
		
		-- log and exec SQL
        IF @ExecSQL = 1
        BEGIN
			IF @DebugMode = 1
				RAISERROR('  Logging actions...', 0, 42) WITH NOWAIT;

			-- get start time for logging
            SET @l_TimeStart = GETDATE();

			-- log actions
            INSERT INTO dbo.T_UTIL_IndxDefragLog (
				[DBId]
				, DBName
				, TblId
				, TblName
				, IndxId
				, IndxName
				, PartitionNumber
				, Fragmentation
				, [PageCount]
				, DateTimeStart
				, SQLStmt
			)
            SELECT	@l_DBId
					, @DBName
					, @l_TblId
					, @l_TblName
					, @l_IndxId
					, @l_IndxName
					, @l_PartitionNumber
					, @l_Fragmentation
					, @l_PageCount
					, @l_TimeStart
					, @l_SQLCommand;

            SET @l_IndxDefragId = SCOPE_IDENTITY();

			IF @PrintCommands = 1
            OR @DebugMode = 1
            BEGIN
				SET @l_DebugMsg = 'Executing: ' + @l_SQLCommand;

                RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
			END;

			BEGIN TRY
				IF ISNULL(@l_DefragChk, 0) = 0
				BEGIN
					-- exec defrag
					EXEC sp_executesql
						@statement = @l_SQLCommand;
					
					-- exec upd statistics
					EXEC sp_executesql
						@statement = @l_StatisticsSQL;
				END;

					-- get end time for logging
					SET @l_TimeEnd = GETDATE();
				
				-- upd log complete
                UPDATE	dbo.T_UTIL_IndxDefragLog
                SET		DateTimeEnd = @l_TimeEnd
                WHERE	IndxDefragId = @l_IndxDefragId;
            END TRY

            BEGIN CATCH
				-- upd log with error
                UPDATE	dbo.T_UTIL_IndxDefragLog
                SET		DateTimeEnd = GETDATE()
						, ErrMsg = ERROR_MESSAGE()
                WHERE	IndxDefragId = @l_IndxDefragId;

                IF @DebugMode = 1 
                    RAISERROR('  An error occurred executing this command. Please review the T_UTIL_IndxDefragLog table for details.', 0, 42) WITH NOWAIT;
            END CATCH;

            UPDATE	dbo.T_UTIL_IndxDefragStatus
            SET		DefragDate = GETDATE()
					, PrintStatus = 1
            WHERE	[DBId] = @l_DBId
			And		TblId = @l_TblId
			And		IndxId = @l_IndxId;
		END;
		-- no exec, just output commands
		ELSE
		BEGIN
            IF @DebugMode = 1
				RAISERROR('  Printing SQL stmts...', 0, 42) WITH NOWAIT;
            
            IF @PrintCommands = 1
            OR @DebugMode = 1
            BEGIN
                SET @l_DebugMsg = ISNULL(@l_SQLCommand,'error');

                RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
                
                SET @l_DebugMsg = ISNULL(@l_StatisticsSQL,'error');

                RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
                
			END;

			UPDATE	dbo.T_UTIL_IndxDefragStatus
			SET		PrintStatus = 1
			WHERE	[DBId] = @l_DBId
			AND		TblId = @l_TblId
			AND		IndxId = @l_IndxId;
		END;
	END;

	-- if exec SQL, then mark affected tables for recompile
	IF @ExecSQL = 1
	BEGIN
        IF @DebugMode = 1
			RAISERROR('  Identifying tbls for recompile...', 0, 42) WITH NOWAIT;

		INSERT INTO @t_RecompileList (DBName, SchemaName, TblName, Processed)
		SELECT	DBName
				, SchemaName
				, TblName
				, 0
        FROM	dbo.T_UTIL_IndxDefragStatus
        WHERE	DefragDate >= @l_StartTime;

		WHILE EXISTS (SELECT 1 FROM @t_RecompileList WHERE Processed = 0)
		BEGIN
			SELECT	TOP 1 @l_RowId = RowId
					, @DBName = DBName
					, @l_SchemaName = SchemaName
					, @l_TblName = TblName
			FROM	@t_RecompileList
			WHERE	Processed = 0;

			SET @l_RecompileSQL = 'USE ' + @DBName + ';
								EXEC sp_recompile N' + '''' + @DBName + N'.' + @l_SchemaName + N'.' + @l_TblName + ''''; 

			EXEC sp_executesql
				@statement = @l_RecompileSQL;

			UPDATE	@t_RecompileList
			SET		Processed = 1
			WHERE	RowId = @l_RowId;
		END;
	END;

	-- return fragmentation results
    IF @PrintFragmentation = 1
    BEGIN
        IF @DebugMode = 1
			RAISERROR('  Displaying summary of action...', 0, 42) WITH NOWAIT;

        SELECT [DBId]
				, DBName
				, TblId
				, TblName
				, IndxId
				, IndxName
				, PartitionNumber
				, Fragmentation
				, [PageCount]
				, RangeScanCount
        FROM	dbo.T_UTIL_IndxDefragStatus
        WHERE	DefragDate >= @l_StartTime
        ORDER BY DefragDate;
	END;
END TRY
	
BEGIN CATCH
	SET @l_DebugMsg = ERROR_MESSAGE() + ' (Line Number: ' + CAST(ERROR_LINE() AS VARCHAR(10)) + ')';
	
	RAISERROR(@l_DebugMsg, 0, 42);
END CATCH;

IF @DebugMode = 1
	RAISERROR('DONE.', 0, 42) WITH NOWAIT;

SET NOCOUNT OFF;
RETURN 0;

GO

SET QUOTED_IDENTIFIER OFF; 
GO
SET ANSI_NULLS ON; 
GO

