USE rp_util;
GO

IF OBJECT_ID('dbo.HealthCheck_LogSpace','P') IS NOT NULL
	DROP PROC dbo.HealthCheck_LogSpace;
GO

CREATE PROC dbo.HealthCheck_LogSpace
	@p_LogHealthCheck BIT = 0
	,@p_SendAlerts BIT = 0
	,@p_CleanUpLog BIT = 0
AS
/************************************************************************************************
DESCRIPTION:
	Audit and alert on SQL Server Log Space

	Code References:
	https://sqlperformance.com/2014/12/io-subsystem/proactive-sql-server-health-checks-1
	http://www.sqlservergeeks.com/sql-server-monitoring-transaction-log-size-with-email-alerts/

PARAM:
	@p_LogHealthCheck - write data to rp_util.dbo.[HealthCheck_chkname]
		- 1: true - logs data, does not return results
		- 0: false - does not log data
	@p_SendAlerts - if applicable, send email alerts 
		- 1: true - if applicabale sends alerts, does not return results
		- 0: false - returns results
	@p_CleanUpLog - if applicable, clean up DBLogSpace table
		- 1: true - purge records > 30 days old
		- 0: false - skip cleanup

USAGE:
	-- return results
	EXEC dbo.HealthCheck_LogSpace
		@p_LogHealthCheck = 0
		,@p_SendAlerts = 0;
			
	-- log data, do not send alerts
	EXEC dbo.HealthCheck_LogSpace
		@p_LogHealthCheck = 1
		,@p_SendAlerts = 0;
		
	-- log data, send alerts
	EXEC dbo.HealthCheck_LogSpace
		@p_LogHealthCheck = 1
		,@p_SendAlerts = 1;
		
	-- do not log data, send alerts
	EXEC dbo.HealthCheck_LogSpace
		@p_LogHealthCheck = 0
		,@p_SendAlerts = 1;
		
	-- clean up log table
	EXEC dbo.HealthCheck_LogSpace
		@p_CleanUpLog = 1;

	SELECT * FROM dbo.DBLogSpace ORDER BY CaptureDate DESC

HISTORY:
	12202016 - neh - sp created

************************************************************************************************/
SET NOCOUNT ON;

DECLARE	@l_SpaceUsedThreshold NUMERIC(9,5) = 70.0
		,@l_XML NVARCHAR(MAX)
		,@l_Body NVARCHAR(MAX)
		,@l_Recipients NVARCHAR(MAX)
		,@l_CleanUpThreshold INT = 30
		,@l_Today DATE = GETDATE();

IF @p_LogHealthCheck = 1
BEGIN
	IF OBJECT_ID('dbo.DBLogSpace','U') IS NULL
	BEGIN
		CREATE TABLE [dbo].[DBLogSpace] (
			[LogSpaceId] INT NOT NULL IDENTITY(1,1)
			,[DatabaseName] [NVARCHAR](128) NOT NULL
			,[LogSizeMB] [INT] NOT NULL
			,[LogSpaceUsed] [NUMERIC](9,5) NOT NULL
			,[CaptureDate] [DATETIME2] NOT NULL
		) ON [PRIMARY];

		ALTER TABLE [dbo].[DBLogSpace] ADD CONSTRAINT PK_DBLogSpace PRIMARY KEY CLUSTERED (LogSpaceId) WITH (FILLFACTOR=100) ON [PRIMARY];
		ALTER TABLE [dbo].[DBLogSpace] ADD CONSTRAINT UK_DBLogSpace_DatabaseName_CaptureDate UNIQUE NONCLUSTERED (DatabaseName, CaptureDate) WITH (FILLFACTOR=100) ON [PRIMARY];
		ALTER TABLE [dbo].[DBLogSpace] ADD CONSTRAINT DF_DBLogSpace_CaptureDate DEFAULT (SYSDATETIME()) FOR [CaptureDate];
	END;
END;

-- collect log file size and free space
DECLARE @tmp_LogSpace TABLE (
     DatabaseName NVARCHAR(128)
    ,LogSizeMB DECIMAL(19,2)
    ,LogSpaceUsed DECIMAL(9,5)
    ,LogStatus CHAR(1)
);

INSERT INTO @tmp_LogSpace (
    DatabaseName
	,LogSizeMB
	,LogSpaceUsed
	,LogStatus
)
EXEC('dbcc sqlperf(logspace)');

IF @p_LogHealthCheck = 1
BEGIN
	INSERT INTO dbo.DBLogSpace (
		DatabaseName
		,LogSizeMB
		,LogSpaceUsed
	)
	SELECT  DatabaseName
		   ,CAST(ROUND(LogSizeMB,0) AS INT)
		   ,LogSpaceUsed
	FROM    @tmp_LogSpace;
END;
ELSE IF @p_SendAlerts = 0 AND @p_CleanUpLog = 0
BEGIN
	SELECT  DatabaseName
		   ,CAST(ROUND(LogSizeMB,0) AS INT) AS LogSizeMB
		   ,LogSpaceUsed
	FROM    @tmp_LogSpace;
END;

IF @p_SendAlerts = 1
BEGIN
    SELECT	@l_XML = CAST((
		SELECT	DatabaseName AS 'td'
				,''
				,CAST(ROUND(LogSizeMB,0) AS INT) AS 'td'
				,''
				,LogSpaceUsed AS 'td'
		FROM	@tmp_LogSpace
		WHERE	LogSpaceUsed > @l_SpaceUsedThreshold
		FOR XML PATH('tr'), ELEMENTS
	) AS NVARCHAR(MAX));

	IF @l_XML IS NOT NULL
	BEGIN
		SET @l_Body ='<html><body><H2>High T-Log Size </H2><table border = 1 BORDERCOLOR="Black"> <tr><th> Database </th> <th> LogSize </th> <th> LogUsed </th> </tr>';
		SET @l_Body = @l_Body + @l_XML + '</table></body></html>';
		
        SELECT  @l_Recipients = email_address
        FROM    msdb.dbo.sysoperators
        WHERE   name = 'DBAs';
		--SET @l_Recipients = 'nate.hughes@morningstar.com';

		EXEC msdb.dbo.sp_send_dbmail
			@recipients = @l_Recipients
			,@body = @l_Body
			,@body_format = 'html'
			,@subject = 'ALERT: High T-Log Size'
			,@importance = 'High';
	END;
END;

IF @p_CleanUpLog = 1
BEGIN
    DELETE
    FROM	dbo.DBLogSpace
    WHERE	DATEDIFF(DAY,CAST(CaptureDate AS DATE),@l_Today) > @l_CleanUpThreshold;
END;

GO