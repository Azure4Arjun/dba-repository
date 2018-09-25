USE [rp_util];
GO

IF OBJECT_ID('dbo.p_admin_CaptureBenchmark','P') IS NOT NULL
	DROP PROC dbo.p_admin_CaptureBenchmark;
GO



CREATE PROC [dbo].[p_admin_CaptureBenchmark]
	@Display BIT = 0
AS
/************************************************************************************************
DESCRIPTION:
	

USAGE:
	-- (Sledgehammer)
	-- Remove all elements from the plan cache for the entire instance 
	-- DBCC FREEPROCCACHE;

	EXEC dbo.p_admin_CaptureBenchmark
		@Display = 1;
	
	SELECT * FROM rp_util.dbo.ProcedureBenchmark ORDER BY 2, 3, CaptureDate DESC;
	SELECT * FROM rp_util.dbo.WaitStats
	SELECT * FROM rp_util.dbo.IOLatency

HISTORY:
	03032016 - neh - sp created
	07202017 - neh - SPs being duplicated in ProcedureBenchmark: records in sys.dm_exec_procedure_stats for same SP called from multiple DBs
					- account for 'divide by zero'

************************************************************************************************/
BEGIN
SET NOCOUNT ON;

IF @Display = 0
BEGIN
	IF NOT EXISTS (
		SELECT	1
		FROM	rp_util.sys.tables
		WHERE	name = 'ProcedureBenchmark'
	)
	BEGIN
		-- DROP TABLE rp_util.dbo.ProcedureBenchmark
		CREATE TABLE rp_util.dbo.ProcedureBenchmark (
			ProcedureBenchmarkId INT NOT NULL IDENTITY(1,1)
			,[Database] NVARCHAR(128)
			,[SP] NVARCHAR(128)
			,[SPCreated] DATE
			,[SPModified] DATE
			,[MinutesInCache] BIGINT -- Time (in minutes) since the stored procedure was added to the cache.
			,[LastExecution] DATETIME -- Last time at which the stored procedure was executed.
			,[ExecutionCount] BIGINT -- Number of times the stored procedure has been executed since it was last compiled.
			,[AvgElapsedTime] BIGINT -- Average elapsed time, in microseconds, for completed executions of this stored procedure.
			,[Calls/Second] BIGINT -- Average number of times per second the stored procedure has been executed since it was compiled.
			,[TotalCPUTime] BIGINT -- Total amount of CPU time, in microseconds, that was consumed by executions of this stored procedure since it was compiled.
			,[MaxCPUTime] BIGINT -- Maximum CPU time, in microseconds, that this stored procedure has ever consumed during a single execution.
			,[AvgCPUTime] BIGINT -- Average amount of CPU time, in microseconds, that was consumed by executions of this stored procedure since it was compiled.
			,[TotalPhysicalReads] BIGINT -- Total number of physical reads performed by executions of this stored procedure since it was compiled.
			,[MaxPhysicalReads] BIGINT -- Maximum number of physical reads that this stored procedure has ever performed during a single execution.
			,[AvgPhysicalReads] BIGINT -- Average number of physical reads performed by executions of this stored procedure since it was compiled.
			,[TotalLogicalReads] BIGINT -- Total number of logical reads performed by executions of this stored procedure since it was compiled.
			,[MaxLogicalReads] BIGINT -- Maximum number of logical reads that this stored procedure has ever performed during a single execution.
			,[AvgLogicalReads] BIGINT -- Average number of logical reads performed by executions of this stored procedure since it was compiled.
			,[TotalLogicalWrites] BIGINT -- Total number of logical writes performed by executions of this stored procedure since it was compiled.
			,[MaxLogicalWrites] BIGINT -- Maximum number of logical writes that this stored procedure has ever performed during a single execution.
			,[AvgLogicalWrites] BIGINT -- Average number of logical writes performed by executions of this stored procedure since it was compiled.
			,[LogicalWrites/Min] BIGINT -- Average number of writes per minute performed by executions of this stored procedure since it was compiled.
			,CaptureDate DATETIME
			,CONSTRAINT PK_ProcedureBenchmark PRIMARY KEY CLUSTERED (ProcedureBenchmarkId) WITH (FILLFACTOR=100) ON [PRIMARY]
		);
	END;
	
	IF NOT EXISTS (
		SELECT	1
		FROM	rp_util.sys.tables
		WHERE	name = 'WaitStats'
	)
	BEGIN
	    -- DROP TABLE rp_util.dbo.WaitStats
		CREATE TABLE rp_util.dbo.WaitStats (
			WaitStatsId INT NOT NULL IDENTITY(1,1)
			,WaitType NVARCHAR(60) -- Name of the wait type
			,Wait_S NUMERIC(14,2) -- Total wait time for this wait type in seconds
			,Resource_S NUMERIC(14,2) -- Total Resource wait time in seconds (Wait - Signal)
			,Signal_S NUMERIC(14,2) -- Difference between the time, in seconds, that the waiting thread was signaled and when it started running
			,WaitCount BIGINT -- Number of waits on this wait type
			,Percentage NUMERIC(5,2) -- Wait type percentage 
			,AvgWait_S AS CAST ((Wait_S / NULLIF(WaitCount,0)) AS NUMERIC(14,4)) 
			,AvgRes_S  AS CAST ((Resource_S / NULLIF(WaitCount,0)) AS NUMERIC(14,4))
			,AvgSig_S  AS CAST ((Signal_S / NULLIF(WaitCount,0)) AS NUMERIC(14,4)) 
			,CaptureDate DATETIME
			,CONSTRAINT PK_WaitStats PRIMARY KEY CLUSTERED (WaitStatsId) WITH (FILLFACTOR=100) ON [PRIMARY]
		);
	END;
	
	IF NOT EXISTS (
		SELECT	1
		FROM	rp_util.sys.tables
		WHERE	name = 'IOLatency'
	)
	BEGIN
		-- DROP TABLE dbo.IOLatency
	    CREATE TABLE dbo.IOLatency (
			IOLatencyId INT NOT NULL IDENTITY(1,1)
			,ReadLatency BIGINT
			,WriteLatency BIGINT
			,Latency BIGINT
			,AvgBytesPerRead BIGINT
			,AvgBytesPerWrite BIGINT
			,AvgBytesPerTransfer BIGINT
			,[Database] NVARCHAR(128)
			,[Filename] NVARCHAR(260)
			,[Path] NVARCHAR(260)
			,CaptureDate DATETIME
			,CONSTRAINT PK_IOLatency PRIMARY KEY CLUSTERED (IOLatencyId) WITH (FILLFACTOR=100) ON [PRIMARY]
		);
	END;

END;

DECLARE @l_SQL NVARCHAR(MAX)
		,@l_WaitStat_DefaultPct NUMERIC(5,2) = 95.0;

DECLARE @SP_STATS TABLE (
[Database] NVARCHAR(128)
,[SP] NVARCHAR(128)
,[SPCreated] DATE
,[SPModified] DATE
,[MinutesInCache] BIGINT
,[LastExecution] DATETIME
,[ExecutionCount] BIGINT
,[AvgElapsedTime] BIGINT
,[Calls/Second] BIGINT
,[TotalCPUTime] BIGINT
,[MaxCPUTime] BIGINT
,[AvgCPUTime] BIGINT
,[TotalPhysicalReads] BIGINT
,[MaxPhysicalReads] BIGINT
,[AvgPhysicalReads] BIGINT
,[TotalLogicalReads] BIGINT
,[MaxLogicalReads] BIGINT
,[AvgLogicalReads] BIGINT
,[TotalLogicalWrites] BIGINT
,[MaxLogicalWrites] BIGINT
,[AvgLogicalWrites] BIGINT
,[LogicalWrites/Min] BIGINT
,CaptureDate DATETIME
);

SET @l_SQL = '
USE [?];
IF DB_NAME() NOT IN (''master'', ''msdb'', ''model'', ''tempdb'', ''ReportServer'', ''ReportServerTempDB'')
BEGIN
	SELECT  DB_NAME() AS [Database]
		   ,p.name AS [SP]
		   ,p.create_date
		   ,p.modify_date 

		   ,DATEDIFF(MINUTE, qs.cached_time, GETDATE()) AS [MinutesInCache]
		   ,qs.last_execution_time AS [LastExecution]
		   ,qs.execution_count AS [ExecutionCount]
		   ,qs.total_elapsed_time / NULLIF(qs.execution_count,0) AS [AvgElapsedTime]
		   ,qs.execution_count / NULLIF(DATEDIFF(SECOND, qs.cached_time, GETDATE()),0) AS [Calls/Second]
	   
		   ,qs.total_worker_time AS [TotalCPUTime]
		   ,qs.max_worker_time AS [MaxCPUTime]
		   ,qs.total_worker_time / NULLIF(qs.execution_count,0) AS [AvgCPUTime]

		   ,qs.total_physical_reads AS [TotalPhysicalReads]
		   ,qs.max_physical_reads AS [MaxPhysicalReads]
		   ,qs.total_physical_reads / NULLIF(qs.execution_count,0) AS [AvgPhysicalReads]
	   
		   ,qs.total_logical_reads AS [TotalLogicalReads]
		   ,qs.max_logical_reads AS [MaxLogicalReads]
		   ,qs.total_logical_reads / NULLIF(qs.execution_count,0) AS [AvgLogicalReads]

		   ,qs.total_logical_writes AS [TotalLogicalWrites]
		   ,qs.max_logical_writes AS [MaxLogicalWrites]
		   ,qs.total_logical_writes / NULLIF(qs.execution_count,0) AS [AvgLogicalWrites]
		   ,qs.total_logical_writes / NULLIF(DATEDIFF(MINUTE, qs.cached_time, GETDATE()),0) AS [LogicalWrites/Min]

		   ,GETDATE() AS CaptureDate
	FROM    sys.procedures p
			LEFT OUTER JOIN sys.dm_exec_procedure_stats qs ON qs.object_id = p.object_id
	WHERE	p.is_ms_shipped = 0
	AND		DB_NAME(qs.database_id) = DB_NAME();
END;
';

INSERT INTO @SP_STATS (
	[Database]
	,SP
	,SPCreated
	,SPModified
	,MinutesInCache
	,LastExecution
	,ExecutionCount
	,AvgElapsedTime
	,[Calls/Second]
	,TotalCPUTime
	,MaxCPUTime
	,AvgCPUTime
	,TotalPhysicalReads
	,MaxPhysicalReads
	,AvgPhysicalReads
	,TotalLogicalReads
	,MaxLogicalReads
	,AvgLogicalReads
	,TotalLogicalWrites
	,MaxLogicalWrites
	,AvgLogicalWrites
	,[LogicalWrites/Min]
	,CaptureDate
)
EXEC sys.sp_MSforeachdb @l_SQL;

IF @Display = 0
BEGIN
	INSERT INTO rp_util.dbo.ProcedureBenchmark (
		[Database]
		,SP
		,SPCreated
		,SPModified
		,MinutesInCache
		,LastExecution
		,ExecutionCount
		,AvgElapsedTime
		,[Calls/Second]
		,TotalCPUTime
		,MaxCPUTime
		,AvgCPUTime
		,TotalPhysicalReads
		,MaxPhysicalReads
		,AvgPhysicalReads
		,TotalLogicalReads
		,MaxLogicalReads
		,AvgLogicalReads
		,TotalLogicalWrites
		,MaxLogicalWrites
		,AvgLogicalWrites
		,[LogicalWrites/Min]
		,CaptureDate
	)
	SELECT	[Database]
		   ,SP
		   ,SPCreated
		   ,SPModified
		   ,MinutesInCache
		   ,LastExecution
		   ,ExecutionCount
		   ,AvgElapsedTime
		   ,[Calls/Second]
		   ,TotalCPUTime
		   ,MaxCPUTime
		   ,AvgCPUTime
		   ,TotalPhysicalReads
		   ,MaxPhysicalReads
		   ,AvgPhysicalReads
		   ,TotalLogicalReads
		   ,MaxLogicalReads
		   ,AvgLogicalReads
		   ,TotalLogicalWrites
		   ,MaxLogicalWrites
		   ,AvgLogicalWrites
		   ,[LogicalWrites/Min]
		   ,CaptureDate
	FROM	@SP_STATS;
	
    WITH    [Waits]
              AS (
                  SELECT    [wait_type]
                           ,[wait_time_ms] / 1000.0 AS [WaitS]
                           ,([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS]
                           ,[signal_wait_time_ms] / 1000.0 AS [SignalS]
                           ,[waiting_tasks_count] AS [WaitCount]
                           ,100.0 * [wait_time_ms]
                            / SUM(NULLIF([wait_time_ms],0)) OVER () AS [Percentage]
                           ,CAST(ROW_NUMBER() OVER (ORDER BY [wait_time_ms] DESC) AS BIGINT) AS [RowNum]
                  FROM      sys.dm_os_wait_stats
                  WHERE     [wait_type] NOT IN (N'CLR_SEMAPHORE',
                                                N'LAZYWRITER_SLEEP',
                                                N'RESOURCE_QUEUE',
                                                N'SQLTRACE_BUFFER_FLUSH',
                                                N'SLEEP_TASK',
                                                N'SLEEP_SYSTEMTASK',
                                                N'WAITFOR',
                                                N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                                                N'CHECKPOINT_QUEUE',
                                                N'REQUEST_FOR_DEADLOCK_SEARCH',
                                                N'XE_TIMER_EVENT',
                                                N'XE_DISPATCHER_JOIN',
                                                N'LOGMGR_QUEUE',
                                                N'FT_IFTS_SCHEDULER_IDLE_WAIT',
                                                N'BROKER_TASK_STOP',
                                                N'CLR_MANUAL_EVENT',
                                                N'CLR_AUTO_EVENT',
                                                N'DISPATCHER_QUEUE_SEMAPHORE',
                                                N'TRACEWRITE',
                                                N'XE_DISPATCHER_WAIT',
                                                N'BROKER_TO_FLUSH',
                                                N'BROKER_EVENTHANDLER',
                                                N'FT_IFTSHC_MUTEX',
                                                N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                                                N'DIRTY_PAGE_POLL',
                                                N'SP_SERVER_DIAGNOSTICS_SLEEP')
                 )

	INSERT INTO dbo.WaitStats (
	    WaitType
		,Wait_S
		,Resource_S
		,Signal_S
		,WaitCount
		,Percentage
		,CaptureDate
	)
	SELECT  [W1].[wait_type] AS [WaitType]
            ,CAST ([W1].[WaitS] AS DECIMAL(14, 2)) AS [Wait_S]
            ,CAST ([W1].[ResourceS] AS DECIMAL(14, 2)) AS [Resource_S]
            ,CAST ([W1].[SignalS] AS DECIMAL(14, 2)) AS [Signal_S]
            ,[W1].[WaitCount] AS [WaitCount]
            ,CAST ([W1].[Percentage] AS DECIMAL(5, 2)) AS [Percentage]
			,GETDATE()
    FROM    [Waits] AS [W1]
            INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
    GROUP BY [W1].[RowNum]
            ,[W1].[wait_type]
            ,[W1].[WaitS]
            ,[W1].[ResourceS]
            ,[W1].[SignalS]
            ,[W1].[WaitCount]
            ,[W1].[Percentage]
    HAVING  SUM([W2].[Percentage]) - [W1].[Percentage] < @l_WaitStat_DefaultPct; -- percentage threshold

	INSERT INTO dbo.IOLatency (
	    ReadLatency
		,WriteLatency
		,Latency
		,AvgBytesPerRead
		,AvgBytesPerWrite
		,AvgBytesPerTransfer
		,[Database]
		,[Filename]
		,[Path]
		,CaptureDate
	)
	 SELECT	--virtual file latency
            ReadLatency = CASE WHEN vfs.num_of_reads = 0 THEN 0
                               ELSE (vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads,0))
                          END
           ,WriteLatency = CASE WHEN vfs.io_stall_write_ms = 0 THEN 0
                                ELSE (vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes,0))
                           END
           ,Latency = CASE WHEN (
                                 vfs.num_of_reads = 0
                                 AND vfs.num_of_writes = 0
                                ) THEN 0
                           ELSE (vfs.io_stall / NULLIF((vfs.num_of_reads + vfs.num_of_writes),0))
                      END
			--avg bytes per IOP
           ,AvgBytesPerRead = CASE WHEN vfs.num_of_reads = 0 THEN 0
                               ELSE (vfs.num_of_bytes_read / NULLIF(vfs.num_of_reads,0))
                          END
           ,AvgBytesPerWrite = CASE WHEN vfs.io_stall_write_ms = 0 THEN 0
                                ELSE (vfs.num_of_bytes_written / NULLIF(vfs.num_of_writes,0))
                           END
           ,AvgBytesPerTransfer = CASE WHEN (
                                         vfs.num_of_reads = 0
                                         AND vfs.num_of_writes = 0
                                        ) THEN 0
                                   ELSE ((vfs.num_of_bytes_read
                                          + vfs.num_of_bytes_written)
                                         / NULLIF((vfs.num_of_reads + vfs.num_of_writes),0))
                              END
           ,DB_NAME(vfs.database_id) AS [Database]
           ,SUBSTRING(mf.physical_name,
                      LEN(mf.physical_name) - CHARINDEX('\',
                                                        REVERSE(mf.physical_name))
                      + 2, 100) AS [Filename]
           ,mf.physical_name AS [Path]
		   ,GETDATE()
    FROM    sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
            JOIN sys.master_files AS mf ON vfs.database_id = mf.database_id
                                           AND vfs.file_id = mf.file_id;

END;
ELSE
BEGIN
	SELECT	[Database]
		   ,SP
		   ,SPCreated
		   ,SPModified
		   ,MinutesInCache
		   ,LastExecution
		   ,ExecutionCount
		   ,AvgElapsedTime * 0.001 AS [AvgElapsedTime(ms)]
		   ,[Calls/Second]
		   ,TotalCPUTime * 0.000001 AS [TotalCPUTime(sec)]
		   ,MaxCPUTime * 0.001 AS [MaxCPUTime(ms)]
		   ,AvgCPUTime * 0.001 AS [AvgCPUTime(ms)]
		   ,TotalPhysicalReads
		   ,MaxPhysicalReads
		   ,AvgPhysicalReads
		   ,TotalLogicalReads
		   ,MaxLogicalReads
		   ,AvgLogicalReads
		   ,TotalLogicalWrites
		   ,MaxLogicalWrites
		   ,AvgLogicalWrites
		   ,[LogicalWrites/Min]
		   ,CaptureDate
	FROM	@SP_STATS
	ORDER BY 1, 2;
	
    WITH    [Waits]
              AS (
                  SELECT    [wait_type]
                           ,[wait_time_ms] / 1000.0 AS [WaitS]
                           ,([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS]
                           ,[signal_wait_time_ms] / 1000.0 AS [SignalS]
                           ,[waiting_tasks_count] AS [WaitCount]
                           ,100.0 * [wait_time_ms]
                            / SUM(NULLIF([wait_time_ms],0)) OVER () AS [Percentage]
                           ,ROW_NUMBER() OVER (ORDER BY [wait_time_ms] DESC) AS [RowNum]
                  FROM      sys.dm_os_wait_stats
                  WHERE     [wait_type] NOT IN (N'CLR_SEMAPHORE',
                                                N'LAZYWRITER_SLEEP',
                                                N'RESOURCE_QUEUE',
                                                N'SQLTRACE_BUFFER_FLUSH',
                                                N'SLEEP_TASK',
                                                N'SLEEP_SYSTEMTASK',
                                                N'WAITFOR',
                                                N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                                                N'CHECKPOINT_QUEUE',
                                                N'REQUEST_FOR_DEADLOCK_SEARCH',
                                                N'XE_TIMER_EVENT',
                                                N'XE_DISPATCHER_JOIN',
                                                N'LOGMGR_QUEUE',
                                                N'FT_IFTS_SCHEDULER_IDLE_WAIT',
                                                N'BROKER_TASK_STOP',
                                                N'CLR_MANUAL_EVENT',
                                                N'CLR_AUTO_EVENT',
                                                N'DISPATCHER_QUEUE_SEMAPHORE',
                                                N'TRACEWRITE',
                                                N'XE_DISPATCHER_WAIT',
                                                N'BROKER_TO_FLUSH',
                                                N'BROKER_EVENTHANDLER',
                                                N'FT_IFTSHC_MUTEX',
                                                N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                                                N'DIRTY_PAGE_POLL',
                                                N'SP_SERVER_DIAGNOSTICS_SLEEP')
                 )
    SELECT  [W1].[wait_type] AS [WaitType]
            ,CAST ([W1].[WaitS] AS DECIMAL(14, 2)) AS [Wait_S]
            ,CAST ([W1].[ResourceS] AS DECIMAL(14, 2)) AS [Resource_S]
            ,CAST ([W1].[SignalS] AS DECIMAL(14, 2)) AS [Signal_S]
            ,[W1].[WaitCount] AS [WaitCount]
            ,CAST ([W1].[Percentage] AS DECIMAL(4, 2)) AS [Percentage]
            ,CAST (([W1].[WaitS] / NULLIF([W1].[WaitCount],0)) AS DECIMAL(14, 4)) AS [AvgWait_S]
            ,CAST (([W1].[ResourceS] / NULLIF([W1].[WaitCount],0)) AS DECIMAL(14, 4)) AS [AvgRes_S]
            ,CAST (([W1].[SignalS] / NULLIF([W1].[WaitCount],0)) AS DECIMAL(14, 4)) AS [AvgSig_S]
    FROM    [Waits] AS [W1]
            INNER JOIN [Waits] AS [W2] ON [W2].[RowNum] <= [W1].[RowNum]
    GROUP BY [W1].[RowNum]
            ,[W1].[wait_type]
            ,[W1].[WaitS]
            ,[W1].[ResourceS]
            ,[W1].[SignalS]
            ,[W1].[WaitCount]
            ,[W1].[Percentage]
    HAVING  SUM([W2].[Percentage]) - [W1].[Percentage] < @l_WaitStat_DefaultPct; -- percentage threshold
		
	 SELECT	--virtual file latency
            ReadLatency = CASE WHEN vfs.num_of_reads = 0 THEN 0
                               ELSE (vfs.io_stall_read_ms / NULLIF(vfs.num_of_reads,0))
                          END
           ,WriteLatency = CASE WHEN vfs.io_stall_write_ms = 0 THEN 0
                                ELSE (vfs.io_stall_write_ms / NULLIF(vfs.num_of_writes,0))
                           END
           ,Latency = CASE WHEN (
                                 vfs.num_of_reads = 0
                                 AND vfs.num_of_writes = 0
                                ) THEN 0
                           ELSE (vfs.io_stall / NULLIF((vfs.num_of_reads + vfs.num_of_writes),0))
                      END
			--avg bytes per IOP
           ,AvgBytesPerRead = CASE WHEN vfs.num_of_reads = 0 THEN 0
                               ELSE (vfs.num_of_bytes_read / NULLIF(vfs.num_of_reads,0))
                          END
           ,AvgBytesPerWrite = CASE WHEN vfs.io_stall_write_ms = 0 THEN 0
                                ELSE (vfs.num_of_bytes_written / NULLIF(vfs.num_of_writes,0))
                           END
           ,AvgBytesPerTransfer = CASE WHEN (
                                         vfs.num_of_reads = 0
                                         AND vfs.num_of_writes = 0
                                        ) THEN 0
                                   ELSE ((vfs.num_of_bytes_read
                                          + vfs.num_of_bytes_written)
                                         / NULLIF((vfs.num_of_reads + vfs.num_of_writes),0))
                              END
           ,DB_NAME(vfs.database_id) AS [Database]
           ,SUBSTRING(mf.physical_name,
                      LEN(mf.physical_name) - CHARINDEX('\',
                                                        REVERSE(mf.physical_name))
                      + 2, 100) AS [Filename]
           ,mf.physical_name AS [Path]
    FROM    sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
            JOIN sys.master_files AS mf ON vfs.database_id = mf.database_id
                                           AND vfs.file_id = mf.file_id
    ORDER BY Latency DESC;

END;

END;
GO


