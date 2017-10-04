USE [msdb];
GO

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'HealthCheck - SQLAgentFailedJob')
	EXEC msdb.dbo.sp_delete_job @job_name=N'HealthCheck - SQLAgentFailedJob', @delete_unused_schedule=1;
GO

DECLARE @output_file_path NVARCHAR(200)
		,@output_file_name NVARCHAR(200)
		,@schedule_id INT;

SET @output_file_path = REPLACE(CAST(SERVERPROPERTY('ErrorLogFileName') AS NVARCHAR(200)), '\ERRORLOG', '');
SELECT  @output_file_path AS ErrorFilePath;

BEGIN TRANSACTION;
DECLARE @ReturnCode INT;
SELECT @ReturnCode = 0;
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance';
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
END;

SET @output_file_name = @output_file_path + N'SQLAgentFailedJob_$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(STRTDT))_$(ESCAPE_SQUOTE(STRTTM)).txt';

EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'HealthCheck - SQLAgentFailedJob', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Checks for failed job steps and sends alert email to DBAs', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'DBAs';
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
/****** Object:  Step [SQL Agent Job Step Alert]    Script Date: 6/21/2017 2:12:36 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_name=N'HealthCheck - SQLAgentFailedJob', @step_name=N'SQL Agent Job Step Alert', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE AS LOGIN = ''SA'';
DECLARE @recipients NVARCHAR(MAX)
		,@subject NVARCHAR(MAX)
		,@body NVARCHAR(MAX)
		,@query NVARCHAR(MAX)
		,@runinterval INT = -30 -- last 30 minutes
		,@runtime INT
		,@rundate INT;
		
SELECT @rundate = CAST(YEAR(DATEADD(MINUTE,@runinterval,GETDATE())) AS CHAR(4))
		+ CASE WHEN LEN(MONTH(DATEADD(MINUTE,@runinterval,GETDATE()))) = 1 THEN ''0'' ELSE '''' END
		+ CAST(MONTH(DATEADD(MINUTE,@runinterval,GETDATE())) AS VARCHAR)
		+ CASE WHEN LEN(DAY(DATEADD(MINUTE,@runinterval,GETDATE()))) = 1 THEN ''0'' ELSE '''' END
		+ CAST(DAY(DATEADD(MINUTE,@runinterval,GETDATE())) AS VARCHAR);

SELECT @runtime = CAST(DATEPART(HOUR,DATEADD(MINUTE,@runinterval,GETDATE())) AS VARCHAR(2))
		+ CASE WHEN LEN(DATEPART(MINUTE,DATEADD(MINUTE,@runinterval,GETDATE()))) = 1 THEN ''0'' ELSE '''' END
		+ CAST(DATEPART(MINUTE,DATEADD(MINUTE,@runinterval,GETDATE())) AS VARCHAR)
		+ ''00''; -- SECONDS

SELECT	@recipients = email_address
FROM	msdb.dbo.sysoperators
WHERE	name = ''DBAs'';

SET @subject = ''SQL Agent Job Failure on '' + CONVERT(NVARCHAR(128),SERVERPROPERTY(''ServerName''));

IF EXISTS(
	SELECT job_name = j.name
			,s.step_id
			,s.step_name
			,s.subsystem
			,s.database_name
			,s.command
			,s.last_run_outcome
			,h.message
	FROM	msdb.dbo.sysjobs j
			INNER JOIN msdb.dbo.sysjobsteps s ON s.job_id = j.job_id
			LEFT OUTER JOIN msdb.dbo.sysjobhistory h
				ON h.job_id = s.job_id
				AND h.step_id = s.step_id
	WHERE	j.enabled = 1
	AND		s.last_run_outcome = 0 -- FAILED
	AND		s.last_run_date >= @rundate
	AND		s.last_run_time >= @runtime
)
BEGIN

SET @query = ''set nocount on;
	SELECT	''''job_name: '''' + j.name
			+ char(10) + char(10) + ''''step_name: '''' + s.step_name
			+ char(10) + char(10) + ''''err_msg: '''' + h.message
	FROM	msdb.dbo.sysjobs j
			INNER JOIN msdb.dbo.sysjobsteps s ON s.job_id = j.job_id
			LEFT OUTER JOIN msdb.dbo.sysjobhistory h
				ON h.job_id = s.job_id
				AND h.step_id = s.step_id
	WHERE	j.enabled = 1
	AND		s.last_run_outcome = 0 -- FAILED
	AND		s.last_run_date >= '' + CAST(@rundate AS VARCHAR)
	+ '' AND	s.last_run_time >= '' + CAST(@runtime AS VARCHAR);
		
EXEC msdb.dbo.sp_send_dbmail
    @recipients = @recipients
    ,@body = @body
    ,@subject = @subject
    ,@importance = ''High''
    ,@query = @query
    ,@query_result_header = 0;

END;', 
		@database_name=N'master', 
		@output_file_name=@output_file_name, 
		@flags=0;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_name=N'HealthCheck - SQLAgentFailedJob', @start_step_id = 1;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
SELECT	@schedule_id = schedule_id
FROM	msdb.dbo.sysschedules
WHERE	name = N'sched: Daily every 2 hours';
IF @schedule_id IS NOT NULL
	EXEC msdb.dbo.sp_attach_schedule @job_name=N'HealthCheck - SQLAgentFailedJob',@schedule_id=@schedule_id;
ELSE
	EXEC msdb.dbo.sp_add_jobschedule @job_name=N'HealthCheck - SQLAgentFailedJob', @name=N'sched: Daily every 2 hours', 
			@enabled=1, 
			@freq_type=4, 
			@freq_interval=1, 
			@freq_subday_type=8, 
			@freq_subday_interval=2, 
			@freq_relative_interval=0, 
			@freq_recurrence_factor=1, 
			@active_start_date=20170621, 
			@active_end_date=99991231, 
			@active_start_time=0, 
			@active_end_time=235959;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_name=N'HealthCheck - SQLAgentFailedJob', @server_name = N'(local)';
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
COMMIT TRANSACTION;
GOTO EndSave;
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION;
EndSave:




