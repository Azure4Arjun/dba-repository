USE NutsAndBolts;
GO

IF OBJECT_ID('dbo.ToggleAGSQLJob', 'U') IS NOT NULL
    DROP TABLE dbo.ToggleAGSQLJob;
GO

CREATE TABLE dbo.ToggleAGSQLJob (
    sysjobname   NVARCHAR(128) NOT NULL
   ,sysjobtoggle BIT           NOT NULL CONSTRAINT DF_ToggleAGSQLJob_sysjobtoggle DEFAULT 0
   ,CONSTRAINT PK_ToggleAGSQLJob PRIMARY KEY CLUSTERED (sysjobname) WITH (FILLFACTOR = 100) ON [PRIMARY]
);

INSERT  dbo.ToggleAGSQLJob (sysjobname, sysjobtoggle)
SELECT  name
       ,0
FROM    msdb.dbo.sysjobs;

--UPDATE dbo.ToggleAGSQLJob SET sysjobtoggle = 1 WHERE sysjobname = '';
--GO

IF OBJECT_ID('dbo.ToggleAGSQLJobs', 'P') IS NOT NULL
    DROP PROC dbo.ToggleAGSQLJobs;
GO

CREATE PROC dbo.ToggleAGSQLJobs
AS
/************************************************************************************************
DESCRIPTION:
	SP will enable/disable SQL Jobs based on which AG node SQL is running on

USAGE:
	EXEC dbo.ToggleAGSQLJobs;
			

HISTORY:
	03022017 - neh - sp created

************************************************************************************************/
BEGIN
SET NOCOUNT ON;

DECLARE @SQL NVARCHAR(MAX) = '';

BEGIN TRY
    -- ADD NEW JOBS
    INSERT  dbo.ToggleAGSQLJob (sysjobname, sysjobtoggle)
    SELECT  j.name
           ,0
    FROM    msdb.dbo.sysjobs                   j
            LEFT OUTER JOIN dbo.ToggleAGSQLJob t
                ON t.sysjobname = j.name
    WHERE   t.sysjobname IS NULL;

    IF master.dbo.fn_hadr_group_is_primary(DEFAULT) = 1 -- PRIMARY
    BEGIN -- PRIMARY NODE
        -- ENABLE ANY SQL JOBS THAT ARE CURRENTLY DISABLED
        SELECT  @SQL += N'EXEC msdb.dbo.sp_update_job @job_name=N''' + t.sysjobname + N''',@enabled=1;'
        FROM    msdb.dbo.sysjobs              j
                INNER JOIN dbo.ToggleAGSQLJob t
                    ON t.sysjobname = j.name
        WHERE   t.sysjobtoggle = 1
        AND     j.enabled = 0;
    END;
    ELSE
    BEGIN -- SECONDARY NODE
        -- DISABLE ANY SQL JOBS THAT ARE CURRENTLY ENABLED
        SELECT  @SQL += N'EXEC msdb.dbo.sp_update_job @job_name=N''' + t.sysjobname + N''',@enabled=0;'
        FROM    msdb.dbo.sysjobs              j
                INNER JOIN dbo.ToggleAGSQLJob t
                    ON t.sysjobname = j.name
        WHERE   t.sysjobtoggle = 1
        AND     j.enabled = 1;
    END;

    IF NULLIF(@SQL, '') IS NOT NULL
        EXEC sys.sp_executesql @SQL;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
    RETURN 0;
END CATCH;
END;
GO
