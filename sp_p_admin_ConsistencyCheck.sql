USE [rp_util];
GO

IF OBJECT_ID('dbo.p_admin_ConsistencyCheck','P') IS NOT NULL
	DROP PROC dbo.p_admin_ConsistencyCheck;
GO


CREATE PROC [dbo].[p_admin_ConsistencyCheck]
	@DBName NVARCHAR(128) = NULL
	, @TblName NVARCHAR(256) = NULL
	, @NoIndex BIT = 0
	, @AllErrorMsgs BIT = 1
	, @NoInfoMsgs BIT = 1
	, @PhysicalOnly BIT = 0
	, @ExecSQL BIT = 1
    , @TimeLimit INT = 720
    , @PrintCommands BIT = 0
    , @PrintResults BIT = 0
    , @DebugMode BIT = 0
AS
/***********************************************************************************************
DESCRIPTION:
	exec and log db consistency checks

PARAMETERS:
	@DBName			- Option to specify a database name; null will return all
	@TblName		- Option to specify a table name; null will return all
	@NoIndex		- 1 = skip NC index checks 
					- 0 = include NC index checks 
	@AllErrorMsgs	- 1 = display all reported errors
					- 0 = do not display reported errors
	@NoInfoMsgs		- 1 = suppress all informational messages
					- 0 = display all informational messages
	@PhysicalOnly	- 1 = run using WITH PHYSICAL_ONLY
					- 0 = run full
	@ExecSQL		- 1 = execute
					- 0 = print command only
    @TimeLimit		- Defaulted to 12 hours - optional limit to how much time can be spent performing index defrags; expressed in minutes.
						NOTE: The time limit is checked BEFORE a consistency check is begun, thus a long check can exceed the time limitation.
	@PrintCommands	- 1 = print commands to screen
					- 0 = do not print commands
    @PrintResults	- 1 = print results to screen
					- 0 = do not print results
    @DebugMode		- 1 = display debug comments; helps with troubleshooting
					- 0 = do not display debug comments


USAGE:
	EXEC dbo.p_admin_ConsistencyCheck
		/*@DBName = 'addrcleanup'
		--, @TblName = 't_batch'
		, */@ExecSQL = 1
		, @PrintCommands = 1
		, @PrintResults = 1
		, @DebugMode = 1


HISTORY:
	03102011 - neh - sp created

***********************************************************************************************/
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;

-- declare variables
DECLARE	@l_DebugMsg NVARCHAR(4000)
        , @l_DBId INT
        , @l_TblId INT
        , @l_TblCheck BIT
        , @l_TblSQL NVARCHAR(4000)
        , @l_TblSQLParam NVARCHAR(4000)
        , @l_CheckTblSQL NVARCHAR(4000)
        , @l_CheckDBSQL NVARCHAR(4000);

-- declare temp tables
DECLARE @t_DBList TABLE (
	[DBId] INT
	, DBName NVARCHAR(128)
	, CheckStatus BIT
);

BEGIN TRY
	-- input parameter validation/default values
	IF @TblName IS NOT NULL
	AND @DBName IS NULL
	BEGIN
		RAISERROR('Error: Specified a table name without specifying a database name.', 0, 42) WITH NOWAIT;
		RETURN -1;
	END;
	ELSE IF @TblName IS NOT NULL
		SET @l_TblCheck = 1;

	IF @NoIndex IS NULL
		SET @NoIndex = 0;
		
	IF @AllErrorMsgs IS NULL
		SET @AllErrorMsgs = 1;
		
	IF @NoInfoMsgs IS NULL
		SET @NoInfoMsgs = 1;

	IF @PhysicalOnly IS NULL
		SET @PhysicalOnly = 0;
		
	IF @ExecSQL IS NULL
		SET @ExecSQL = 1;

	IF @TimeLimit IS NULL
		SET @TimeLimit = 720;

	IF @PrintCommands IS NULL
		SET @PrintCommands = 0;

	IF @PrintResults IS NULL
		SET @PrintResults = 0;

	IF @DebugMode IS NULL
		SET @DebugMode = 0;

	IF @DebugMode = 1
		RAISERROR('Consistency check functionality starting up...', 0, 42) WITH NOWAIT;

	IF @DebugMode = 1
		RAISERROR('Grabbing db list...', 0, 42) WITH NOWAIT;

	-- retrieve db list
    INSERT INTO @t_DBList ([DBId], DBName, CheckStatus)
    SELECT	database_id
			, name
			, 0 -- not checked for consistency
    FROM	sys.databases
    WHERE	name = IsNull(@DBName, name)
	AND		[state] = 0 -- state must be ONLINE
	AND		is_read_only = 0; -- cannot be READ_ONLY

    IF @DebugMode = 1
		RAISERROR('Looping thru list of dbs running consistency check...', 0, 42) WITH NOWAIT;

	WHILE EXISTS (SELECT 1 FROM @t_DBList WHERE CheckStatus = 0)
	BEGIN
        SELECT	TOP (1) @l_DBId = [DBId]
        FROM	@t_DBList
        WHERE	CheckStatus = 0;
	
        IF @DebugMode = 1
        BEGIN
			SELECT @l_DebugMsg = '  working on ' + DB_Name(@l_DBId) + '...';
			RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
		END;

		-- retrieve table id, if specified
		IF @l_TblCheck = 1
		BEGIN
			IF @DebugMode = 1
			BEGIN
				SELECT @l_DebugMsg = '  grabbing id for ' + @TblName + '...';
				RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
			END;

			SET @l_TblSQL = N'SELECT	@p_TblId_Out = object_id
							FROM	' + DB_Name(@l_DBId) + '.sys.objects
							WHERE	name = ' + '''' + @TblName + '''';
			
			SET @l_TblSQLParam = N'@p_TblId_Out INT OUTPUT';

            EXEC sp_executesql
				@statement = @l_TblSQL
                , @params = @l_TblSQLParam
                , @p_TblId_Out = @l_TblId OUTPUT;

			IF @DebugMode = 1
			BEGIN
				SELECT @l_DebugMsg = '  preparing DBCC call for ' + @TblName + '...';
				RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
			END;

			SET @l_CheckTblSQL = 'USE ' + DB_Name(@l_DBId) + '; DBCC CHECKTABLE ("' + @TblName + '"';
			
			IF @NoIndex = 1
				SET @l_CheckTblSQL = @l_CheckTblSQL + 'NOINDEX';
			
			SET @l_CheckTblSQL = @l_CheckTblSQL + ')';

			IF @AllErrorMsgs = 1
			OR @NoInfoMsgs = 1
			OR @PhysicalOnly = 1
			BEGIN
				SET @l_CheckTblSQL = @l_CheckTblSQL + ' WITH';
				
				IF @AllErrorMsgs = 1
					SET @l_CheckTblSQL = @l_CheckTblSQL + ' ALL_ERRORMSGS,';
					
				IF @NoInfoMsgs = 1
					SET @l_CheckTblSQL = @l_CheckTblSQL + ' NO_INFOMSGS,';
					
				IF @PhysicalOnly = 1
					SET @l_CheckTblSQL = @l_CheckTblSQL + ' PHYSICAL_ONLY,';
					
				--remove trailing comma
				SET @l_CheckTblSQL = LEFT(@l_CheckTblSQL,LEN(@l_CheckTblSQL)-1);
			END;

			IF @ExecSQL = 0
				-- print DBCC command
				PRINT @l_CheckTblSQL;
			ELSE
			-- exec DBCC command
			BEGIN
				IF @DebugMode = 1
				OR @PrintCommands = 1
				BEGIN
					SELECT @l_DebugMsg = '  executing: ' + @l_CheckTblSQL;
					RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
				END;

				EXEC sp_executesql
					@statement = @l_CheckTblSQL;
			END;                
		END;
		ELSE
		BEGIN
			IF @DebugMode = 1
			BEGIN
				SELECT @l_DebugMsg = '  preparing DBCC call for ' + DB_Name(@l_DBId) + '...';
				RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
			END;

			SET @l_CheckDBSQL = 'DBCC CHECKDB (' + DB_Name(@l_DBId);
			
			IF @NoIndex = 1
				SET @l_CheckDBSQL = @l_CheckDBSQL + 'NOINDEX';
			
			SET @l_CheckDBSQL = @l_CheckDBSQL + ')';

			IF @AllErrorMsgs = 1
			OR @NoInfoMsgs = 1
			OR @PhysicalOnly = 1
			BEGIN
				SET @l_CheckDBSQL = @l_CheckDBSQL + ' WITH';
				
				IF @AllErrorMsgs = 1
					SET @l_CheckDBSQL = @l_CheckDBSQL + ' ALL_ERRORMSGS,';
					
				IF @NoInfoMsgs = 1
					SET @l_CheckDBSQL = @l_CheckDBSQL + ' NO_INFOMSGS,';
					
				IF @PhysicalOnly = 1
					SET @l_CheckDBSQL = @l_CheckDBSQL + ' PHYSICAL_ONLY,';
					
				--remove trailing comma
				SET @l_CheckDBSQL = LEFT(@l_CheckDBSQL,LEN(@l_CheckDBSQL)-1);
			END;

			IF @ExecSQL = 0
				-- print DBCC command
				PRINT @l_CheckDBSQL;
			ELSE
			-- exec DBCC command
			BEGIN
				IF @DebugMode = 1
				OR @PrintCommands = 1
				BEGIN
					SELECT @l_DebugMsg = '  executing: ' + @l_CheckDBSQL;
					RAISERROR(@l_DebugMsg, 0, 42) WITH NOWAIT;
				END;

				EXEC sp_executesql
					@statement = @l_CheckDBSQL;
			END;
		END;

		UPDATE	@t_DBList
		SET		CheckStatus = 1
		WHERE	[DBId] = @l_DBId;
	END;
END TRY
	
BEGIN CATCH
	SET @l_DebugMsg = ERROR_MESSAGE() + ' (Line Number: ' + CAST(ERROR_LINE() AS VARCHAR(10)) + ')';
	RAISERROR(@l_DebugMsg, 0, 42);
END CATCH;

IF @DebugMode = 1
	RAISERROR('DONE.', 0, 42) WITH NOWAIT;

RETURN 0;
END;
GO


