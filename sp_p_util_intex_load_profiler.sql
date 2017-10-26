USE rp_util
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[p_util_intex_load_profiler]') AND OBJECTPROPERTY(object_id, N'IsProcedure') = 1)
	DROP PROCEDURE [dbo].[p_util_intex_load_profiler]
GO

CREATE PROC dbo.p_util_intex_load_profiler
	@TraceFile NVARCHAR(245)
	, @MaxFileSize BIGINT = 5 -- default 5MB
	, @StopTime DATETIME
	, @DatabaseName NVARCHAR(128)
	, @TraceType INT
	, @DurationFilter BIGINT = NULL
AS
/*******************************************************************************
DESCRIPTION:
	INTEX Load Profiler scripted out and dropped into a stored proc

PARAMETERS:
	@TraceFile	- location and file name, .trc extension will be appended automatically,
					to which the trace will be written
	@Option		- option(s) set for trace
	@MaxFileSize	- max size for trace file(s) in  MB
	@StopTime	- date and time the trace will be stopped
	@DatabaseName	- database to be traced
	@TraceType	- pre-defined trace types
				1: INTEX Load
				2: Performance Metrics
				3: Identify SPs/FNs
	@DurationFilter	- duration filter in MS

USAGE:
	EXEC dbo.p_util_intex_load_profiler
		@TraceFile = 'T:\SQL_TRACE\TraceFile'
		, @StopTime = '01/03/2013 10:00'
		, @DatabaseName = 'RMBS'
		, @TraceType = 2

	SELECT * FROM sys.fn_trace_getinfo(0)
	EXEC sp_trace_setstatus @traceid = 2, @status = 0;--stop
	EXEC sp_trace_setstatus @traceid = 2, @status = 2;--delete
	
HISTORY:
	01182007 - neh - sp created
	09072007 - neh - expanded functionality
	12192007 - neh - added ClientHostName to Performance Metrics trace
	11102010 - neh - added Lock:Escalation and SP:Recompile
	01032013 - neh - replace GOTO logic with TRY/CATCH
					- change @Option from input parameter to CONST

***************************************************************************************/
SET NOCOUNT ON;

DECLARE	@l_ReturnCode TINYINT
	, @l_TraceID INT
	, @l_DBFilter INT
	, @l_CONST_On BIT = 1 -- default "Event is turned ON"
	, @l_CONST_Option INT = 2 -- default TRACE_FILE_ROLLOVER
	, @l_CONST_Event_SPStarting INT = 42
	, @l_CONST_Event_SPCompleted INT = 43
	, @l_CONST_Event_RPCCompleted INT = 10
	, @l_CONST_Event_SQLStmtCompleted INT = 41
	, @l_CONST_Column_SPID INT = 12
	, @l_CONST_Column_TextData INT = 1
	, @l_CONST_Column_StartTime INT = 14
	, @l_CONST_Column_EndTime INT = 15
	, @l_CONST_Column_Duration INT = 13
	, @l_CONST_Column_Reads INT = 16
	, @l_CONST_Column_Writes INT = 17
	, @l_CONST_Column_CPU INT = 18
	, @l_CONST_Column_HostName INT = 8
	, @l_CONST_Column_LoginName INT = 11
	, @l_CONST_Column_ObjectId INT = 22
	, @l_CONST_Column_DatabaseId INT = 3
	, @l_CONST_StartTrace INT = 1
	, @l_CONST_DeleteTrace INT = 2
	;

BEGIN TRY
	-- create a new trace (in a stopped state)
	EXEC @l_ReturnCode = sp_trace_create
		@traceid = @l_TraceID output
		, @options = @l_CONST_Option
		, @tracefile = @TraceFile
		, @maxfilesize = @MaxFileSize
		, @stoptime = @StopTime;
END TRY

BEGIN CATCH
	SELECT	SP = 'sp_trace_create'
			, ReturnCode = @l_ReturnCode
			, ErrorNumber = ERROR_NUMBER()
			, ErrorMessage = ERROR_MESSAGE();

	RETURN 0;
END CATCH

BEGIN TRY
-- Set the Trace Events
IF @TraceType = 1 -- INTEX Load
BEGIN
	---- SP:Starting
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPStarting, @columnid = @l_CONST_Column_SPID, @on = @l_CONST_On;
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPStarting, @columnid = @l_CONST_Column_TextData, @on = @l_CONST_On;
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPStarting, @columnid = @l_CONST_Column_StartTime, @on = @l_CONST_On;
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = l_CONST_Event_SPStarting, @columnid = @l_CONST_Column_EndTime, @on = @l_CONST_On;
	-- SP:Completed
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_SPID, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_TextData, @on = @l_CONST_On;
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_EndTime, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_Duration, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_Reads, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_Writes, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SPCompleted, @columnid = @l_CONST_Column_CPU, @on = @l_CONST_On;
END
ELSE IF @TraceType = 2 -- Performance Metrics
BEGIN
	-- RPC:Completed
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_SPID, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_HostName, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_TextData, @on = @l_CONST_On;
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_EndTime, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_Duration, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_Reads, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_Writes, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_RPCCompleted, @columnid = @l_CONST_Column_CPU, @on = @l_CONST_On;
	-- SQL:StmtCompleted
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_SPID, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_HostName, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_LoginName, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_TextData, @on = @l_CONST_On;
	--EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_EndTime, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_Duration, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_Reads, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_Writes, @on = @l_CONST_On;
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = @l_CONST_Event_SQLStmtCompleted, @columnid = @l_CONST_Column_CPU, @on = @l_CONST_On;
END
ELSE IF @TraceType = 3 -- Identify SPs/FNs
BEGIN
	-- SP:Starting
	EXEC @l_ReturnCode = sp_trace_setevent @traceid = @l_TraceID, @eventid = l_CONST_Event_SPStarting, @columnid = @l_CONST_Column_ObjectId, @on = @l_CONST_On;
END
END TRY

BEGIN CATCH
	SELECT	SP = 'sp_trace_create'
			, ReturnCode = @l_ReturnCode
			, ErrorNumber = ERROR_NUMBER()
			, ErrorMessage = ERROR_MESSAGE();
	
	-- close/delete trace
	EXEC sp_trace_setstatus
		@traceid = @l_TraceID
		, @status = @l_CONST_DeleteTrace;
END CATCH

BEGIN TRY
-- Set the Database Filter
	SET @l_DBFilter = DB_ID(@DatabaseName);

	EXEC @l_ReturnCode = sp_trace_setfilter
		@traceid = @l_TraceID
		, @columnid = @l_CONST_Column_DatabaseId
		, @logical_operator = 0 -- AND
		, @comparison_operator = 0 -- = (Equal)
		, @value = @l_DBFilter;
END TRY

BEGIN CATCH
	SELECT	SP = 'sp_trace_setfilter - DB Filter'
			, ReturnCode = @l_ReturnCode
			, ErrorNumber = ERROR_NUMBER()
			, ErrorMessage = ERROR_MESSAGE();
	
	-- close/delete trace
	EXEC sp_trace_setstatus
		@traceid = @l_TraceID
		, @status = @l_CONST_DeleteTrace;
END CATCH

BEGIN TRY
-- Set the Duration Filter
	SET @DurationFilter = ISNULL(@DurationFilter,0);

	EXEC @l_ReturnCode = sp_trace_setfilter
		@traceid = @l_TraceID
		, @columnid = @l_CONST_Column_Duration
		, @logical_operator = 0 -- AND
		, @comparison_operator = 4 -- >= (Greater Than Or Equal)
		, @value = @DurationFilter;
END TRY

BEGIN CATCH
	SELECT	SP = 'sp_trace_setfilter - Duration Filter'
			, ReturnCode = @l_ReturnCode
			, ErrorNumber = ERROR_NUMBER()
			, ErrorMessage = ERROR_MESSAGE();
	
	-- close/delete trace
	EXEC sp_trace_setstatus
		@traceid = @l_TraceID
		, @status = @l_CONST_DeleteTrace;
END CATCH

--BEGIN TRY
---- Set TextData Filters
--	EXEC @l_ReturnCode = sp_trace_setfilter
--		@traceid = @l_TraceID
--		, @columnid = @l_CONST_Column_TextData
--		, @logical_operator = 0
--		, @comparison_operator = 1 /*<> (Not Equal)*/
--		, @value = N'EXECUTE msdb.dbo.sp_sqlagent_get_perf_counters';
--END TRY

--BEGIN CATCH
--	SELECT	SP = 'sp_trace_setfilter - TextData Filter'
--			, ReturnCode = @l_ReturnCode
--			, ErrorNumber = ERROR_NUMBER()
--			, ErrorMessage = ERROR_MESSAGE();
	
--	-- close/delete trace
--	EXEC sp_trace_setstatus
--		@traceid = @l_TraceID
--		, @status = @l_CONST_DeleteTrace;
--END CATCH

BEGIN TRY
-- Start Trace
	EXEC @l_ReturnCode = sp_trace_setstatus
		@traceid = @l_TraceID
		, @status = @l_CONST_StartTrace;
END TRY

BEGIN CATCH
	SELECT	SP = 'sp_trace_setstatus'
			, ReturnCode = @l_ReturnCode
			, ErrorNumber = ERROR_NUMBER()
			, ErrorMessage = ERROR_MESSAGE();
	
	-- close/delete trace
	EXEC sp_trace_setstatus
		@traceid = @l_TraceID
		, @status = @l_CONST_DeleteTrace;
END CATCH

-- Display Trace ID for future reference
SELECT TraceID = @l_TraceID;

GO
