USE rp_util;
GO

IF EXISTS (SELECT   1 FROM  sys.objects WHERE   name = 'usp_AutoRestoreDBs' AND type = 'P')
    DROP PROC dbo.usp_AutoRestoreDBs;
GO

CREATE PROC dbo.usp_AutoRestoreDBs
	@SendEmail BIT = 1
AS
BEGIN
SET NOCOUNT ON;

--IF EXISTS (SELECT 1 FROM rp_util.sys.tables WHERE name = 'T_DB_RESTORE_LOG')
--	DROP TABLE rp_util.dbo.T_DB_RESTORE_LOG;

IF NOT EXISTS (SELECT   1 FROM  rp_util.sys.tables WHERE name = 'T_DB_RESTORE_LOG')
    CREATE TABLE rp_util.dbo.T_DB_RESTORE_LOG (
        RestoreId     INT            IDENTITY(1, 1)
       ,RestoreDate   DATETIME       NOT NULL
       ,DBName        NVARCHAR(128)  NOT NULL
       ,RestoreStatus TINYINT        NOT NULL
       ,AsOfDate      DATETIME       NULL
       ,Duration_MS   INT            NULL
       ,ErrorMessage  NVARCHAR(2048) NULL
       ,CONSTRAINT pk_T_DB_RESTORE_LOG PRIMARY KEY CLUSTERED (RestoreId) WITH (FILLFACTOR = 95)
       ,CONSTRAINT uq_T_DB_RESTORE_LOG_RestoreDate_DBName UNIQUE NONCLUSTERED (RestoreDate, DBName) WITH (FILLFACTOR = 95)
    );

EXEC sys.sp_configure @configname = 'show advanced options'
                     ,@configvalue = 1;
RECONFIGURE;
EXEC sys.sp_configure @configname = 'xp_cmdshell', @configvalue = 1;
RECONFIGURE;

DECLARE @l_SQL          NVARCHAR(4000)
       ,@l_logpath      NVARCHAR(4000)
       ,@l_logpartition NCHAR(1);

EXEC master.sys.xp_instance_regread N'HKEY_LOCAL_MACHINE'
                                   ,N'Software\Microsoft\MSSQLServer\MSSQLServer'
                                   ,N'DefaultLog'
                                   ,@l_logpath OUTPUT
                                   ,'no_output';

SET @l_logpartition = LEFT(@l_logpath, 1);

SET @l_SQL = N'PowerShell.exe -noprofile -command "Get-ChildItem T:\SQL_BAK\*.bak"';

DECLARE @SQL_BAK TABLE (DirList NVARCHAR(4000));

CREATE TABLE #BAK_LIST (
    dbtimestamp DATETIME
   ,dbname      NVARCHAR(128)
   ,bakfile     NVARCHAR(128)
   ,dbbaktype   BIT
   ,PRIMARY KEY CLUSTERED (dbname, dbbaktype)
);

INSERT INTO @SQL_BAK (DirList)
EXEC sys.xp_cmdshell @l_SQL;

--SELECT CONVERT(DATETIME,LTRIM(RTRIM(SUBSTRING('-a---         3/26/2012   9:59 AM     345600 cmp.bak                           ',6,30))))
--		, SUBSTRING(LTRIM(RTRIM(SUBSTRING('-a---         3/26/2012   9:59 AM     345600 cmp.bak                           ',36,4000)))
--			, PATINDEX('%[a-z]%', LTRIM(RTRIM(SUBSTRING('-a---         3/26/2012   9:59 AM     345600 cmp.bak                           ',36,4000))))
--			, 4000)

INSERT INTO #BAK_LIST (dbtimestamp, dbname, bakfile, dbbaktype)
SELECT  tmp.dbtimestamp
       ,tmp.dbname
       ,tmp.bakfile
       ,tmp.dbbaktype
FROM    (
            SELECT  CONVERT(DATETIME, LTRIM(RTRIM(SUBSTRING(DirList, 6, 29)))) AS dbtimestamp
                   ,REPLACE(
                               REPLACE(
                                          SUBSTRING(
                                                       LTRIM(RTRIM(SUBSTRING(DirList, 36, 4000)))
                                                      ,PATINDEX('%[a-z]%', LTRIM(RTRIM(SUBSTRING(DirList, 36, 4000))))
                                                      ,4000
                                                   ), '.bak', ''
                                      ), '_diff', ''
                           )                                                   AS dbname
                   ,SUBSTRING(
                                 LTRIM(RTRIM(SUBSTRING(DirList, 36, 4000)))
                                ,PATINDEX('%[a-z]%', LTRIM(RTRIM(SUBSTRING(DirList, 36, 4000)))), 4000
                             )                                                 AS bakfile
                   ,CASE WHEN DirList LIKE '%_diff%' THEN 1
                         ELSE 0
                    END                                                        AS dbbaktype
            FROM    @SQL_BAK
            WHERE  DirList LIKE '-a---%'
        )                                  tmp
        INNER JOIN dbo.T_DB_RESTORE_DRIVER d
            ON tmp.dbname = d.dbname
        LEFT OUTER JOIN (
                            SELECT  DBName
                                   ,MAX(AsOfDate) AS AsOfDate
                            FROM    dbo.T_DB_RESTORE_LOG
                            WHERE  RestoreStatus = 1
                            GROUP BY DBName
                        )                  l
            ON tmp.dbname = l.dbname
WHERE   d.Active = 1
AND     tmp.dbtimestamp > ISNULL(l.AsOfDate, '1/1/1901');

EXEC sys.sp_configure @configname = 'xp_cmdshell', @configvalue = 0;
RECONFIGURE;
EXEC sys.sp_configure @configname = 'show advanced options'
                     ,@configvalue = 0;
RECONFIGURE;

DECLARE @BAK_STMTS TABLE (
    RowId     INT           IDENTITY(1, 1) PRIMARY KEY CLUSTERED
   ,DBName    NVARCHAR(128)
   ,AsOfDate  DATETIME
   ,BakStmt   NVARCHAR(MAX)
   ,BakStatus TINYINT       DEFAULT 0
   ,Duration  INT
   ,ErrMsg    NVARCHAR(2048)
);

INSERT INTO @BAK_STMTS (DBName, AsOfDate, BakStmt)
SELECT  b1.dbname
       ,b1.dbtimestamp
       ,CASE WHEN b1.dbbaktype = 0 THEN
                 'ALTER DATABASE [' + b1.dbname + '] SET OFFLINE WITH ROLLBACK IMMEDIATE; EXEC sp_detach_db '''
                 + b1.dbname + ''', ''true'';'
             ELSE ''
        END
        + CASE WHEN b2.dbname IS NOT NULL THEN
                   N'RESTORE DATABASE [' + b1.dbname + '] FROM  DISK = N''T:\SQL_BAK\' + b1.bakfile
                   + ''' WITH  FILE = 1,  MOVE N''' + f.name + ''' TO N''' + @l_logpartition
                   + RIGHT(f.physical_name, LEN(f.physical_name) - 1) + ''',  NORECOVERY,  NOUNLOAD,  REPLACE;'
               WHEN b1.dbname = 'Intex_Data' THEN
                   N'RESTORE DATABASE [' + b1.dbname + '] FROM  DISK = N''T:\SQL_BAK\' + b1.bakfile
                   + ''' WITH  FILE = 1,  MOVE N''' + b1.dbname + ''' TO N''D:\SQL_DATA\' + b1.dbname
                   + '.mdf'', NOUNLOAD,  REPLACE;'
               ELSE
                   N'RESTORE DATABASE [' + b1.dbname + '] FROM  DISK = N''T:\SQL_BAK\' + b1.bakfile
                   + ''' WITH  FILE = 1,  MOVE N''' + f.name + ''' TO N''' + @l_logpartition
                   + RIGHT(f.physical_name, LEN(f.physical_name) - 1) + ''', NOUNLOAD,  REPLACE;'
          END
FROM    #BAK_LIST                   b1
        INNER JOIN sys.databases    d
            ON d.name = b1.dbname
        INNER JOIN sys.master_files f
            ON f.database_id = d.database_id
        LEFT OUTER JOIN #BAK_LIST   b2
            ON  b1.dbname = b2.dbname
            AND b1.dbtimestamp < b2.dbtimestamp
            AND b2.dbbaktype = 1
WHERE   f.type = 1; -- LOG

--SELECT * FROM #BAK_LIST
----SELECT * FROM @BAK_FILELIST
--SELECT * FROM @BAK_STMTS

DROP TABLE #BAK_LIST;

DECLARE @l_dbname    NVARCHAR(128)
       ,@l_bakstmt   NVARCHAR(MAX)
       ,@l_rowid     INT
       ,@l_starttime DATETIME
       ,@l_endtime   DATETIME
       ,@l_ParamDef  NVARCHAR(255) = N'@DropStmt NVARCHAR(MAX) OUTPUT'
       --, @l_SQL NVARCHAR(MAX)
       ,@l_DropStmt  NVARCHAR(MAX)
       ,@l_GrantStmt NVARCHAR(MAX);

SELECT  @l_rowid = MIN(RowId)
FROM    @BAK_STMTS
WHERE   BakStatus = 0;

WHILE EXISTS (SELECT    1 FROM  @BAK_STMTS WHERE RowId >= @l_rowid)
BEGIN
    BEGIN TRY
        SELECT  @l_dbname    = DBName
               ,@l_bakstmt   = BakStmt
               ,@l_starttime = GETDATE()
               ,@l_SQL       = N''
               ,@l_DropStmt  = N''
               ,@l_GrantStmt = N''
        FROM    @BAK_STMTS
        WHERE   RowId = @l_rowid;

        EXEC sys.sp_executesql @statement = @l_bakstmt;

        SET @l_SQL =
            N'	SET @DropStmt = N'''';
					SELECT	@DropStmt = @DropStmt + ISNULL(''DROP USER ['' + name + ''];'',''NULL'')
					FROM	' + @l_dbname
            + N'.sys.database_principals
					WHERE	[type] IN (''S'', ''U'', ''G'') -- SQL_USER, WINDOWS_USER, WINDOWS_GROUP
					AND		name NOT IN (''dbo'', ''guest'', ''INFORMATION_SCHEMA'', ''sys'', ''##MS_PolicyEventProcessingLogin##'', ''NT AUTHORITY\NETWORK SERVICE'')';

        EXEC sys.sp_executesql @statement = @l_SQL
                              ,@params = @l_ParamDef
                              ,@DropStmt = @l_DropStmt OUTPUT;

        SET @l_DropStmt = N'USE ' + @l_dbname + N'; EXEC sp_changedbowner ''sa'';' + @l_DropStmt;

        EXEC sys.sp_executesql @statement = @l_DropStmt;

        SELECT  @l_GrantStmt = N'USE ' + @l_dbname + N';' + PermissionStmt
        FROM    rp_util.dbo.T_DB_RESTORE_PERMISSIONS
        WHERE   DBName = @l_dbname;

        EXEC sys.sp_executesql @statement = @l_GrantStmt;

        SET @l_endtime = GETDATE();

        UPDATE  @BAK_STMTS
        SET BakStatus = 1
           ,Duration = DATEDIFF(MS, @l_starttime, @l_endtime)
        WHERE   RowId = @l_rowid;
    END TRY
    BEGIN CATCH
        UPDATE  @BAK_STMTS
        SET BakStatus = 2
           ,ErrMsg = ERROR_MESSAGE()
        WHERE   DBName = @l_dbname;
    END CATCH;

    SELECT  @l_rowid = MIN(RowId)
    FROM    @BAK_STMTS
    WHERE   BakStatus = 0
    AND     RowId > @l_rowid;

END;

DECLARE @l_RunDate DATETIME = GETDATE();

INSERT INTO rp_util.dbo.T_DB_RESTORE_LOG (RestoreDate, DBName, AsOfDate, RestoreStatus, Duration_MS, ErrorMessage)
SELECT  DISTINCT
        @l_RunDate
       ,DBName
       ,MAX(AsOfDate)
       ,BakStatus
       ,SUM(Duration)
       ,ErrMsg
FROM    @BAK_STMTS
GROUP BY DBName
        ,BakStatus
        ,ErrMsg;

DECLARE @recipients NVARCHAR(MAX)
       ,@query      NVARCHAR(MAX);

SELECT  @recipients = N'RealpointIT@morningstar.com;';

SELECT  RestoreDate
       ,CONVERT(CHAR(25), DBName)                                         AS DBName
       ,CONVERT(CHAR(8), CASE RestoreStatus WHEN 1 THEN 'Y' ELSE 'N' END) AS Restored
       ,AsOfDate
       ,CONVERT(CHAR(13), CONVERT(INT, Duration_MS * 0.001))              AS [Duration(sec)]
       ,CONVERT(CHAR(50), ISNULL(ErrorMessage, 'N/A'))                    AS ErrorMessage
INTO    ##RestoreLog
FROM    rp_util.dbo.T_DB_RESTORE_LOG
WHERE   RestoreDate = @l_RunDate;

IF @SendEmail = 1
BEGIN
    SET @query = N'SELECT * FROM ##RestoreLog';

    EXEC msdb.dbo.sp_send_dbmail @profile_name = 'Database Mail'
                                ,@recipients = @recipients
                                ,@subject = 'RPCMBSDRDB82: CMBS DB Restore'
                                ,@query = @query
                                ,@attach_query_result_as_file = 0
                                ,@query_result_header = 1;
END;
ELSE
    SELECT  *
    FROM    ##RestoreLog;

DROP TABLE ##RestoreLog;
END;
GO

