USE [dba_rep]
GO

IF OBJECT_ID('dbo.p_admin_DecipherDBFileSizes','P') IS NOT NULL
	DROP PROC dbo.p_admin_DecipherDBFileSizes;
GO

CREATE PROC dbo.p_admin_DecipherDBFileSizes
	@DBServerName NVARCHAR(128) = NULL
	, @DBName NVARCHAR(128) = NULL
	, @IsSnapshot BIT = 0
	, @OmitTempDb BIT = 1
AS
/*******************************************************************************
DESCRIPTION:
	report on captured file size, free space and VLF counts for requested servers/dbs

PARAMETERS:
	@DBServerName	- target DB server name, if NULL return all active servers
	@DBName			- target DB name, if NULL return all
	@IsSnapshot		- T/F - limit to most recent reported data for server/db
	@OmitTempDb		- T/F - omit TempDb(s) from result

USAGE:
	EXEC [dbo].[p_admin_DecipherDBFileSizes]
		@DBServerName = 'RPBETASQLVS2'
		, @DBName = NULL--'Geography'
		, @IsSnapshot = 1
		, @OmitTempDb = 1;

HISTORY:
	03052012 - neh - sp created
	09142016 - neh - modified to return Prev, SixMonth and TwelveMonth numbers
	
***************************************************************************************/
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET QUOTED_IDENTIFIER ON;

DECLARE @l_MaxAsOfDate DATETIME;

DECLARE @Servers TABLE (ServerName NVARCHAR(128) PRIMARY KEY);

DECLARE @Databases TABLE (
	DBName NVARCHAR(128)
	,FileGroup NVARCHAR(128)
	,PRIMARY KEY (DBName, FileGroup)
);

DECLARE @t_Drvr TABLE (
	ServerName NVARCHAR(128) PRIMARY KEY
	, MaxAsOfDate SMALLDATETIME
	, PrevMonthAsOfDate SMALLDATETIME
	, SixMonthAsOfDate SMALLDATETIME
	, TwelveMonthAsOfDate SMALLDATETIME
	, MaxBakDate SMALLDATETIME
	, PrevMonthBakDate SMALLDATETIME
	, SixMonthBakDate SMALLDATETIME
	, TwelveMonthBakDate SMALLDATETIME
);

-- identify requested server names
IF @DBServerName IS NULL
	INSERT INTO @Servers (ServerName)
	SELECT	HostName
	FROM	dbo.ServerList
	WHERE	Active = 1;
ELSE
	INSERT INTO @Servers (ServerName)
	SELECT	HostName
	FROM	dbo.ServerList
	WHERE	ServerName = @DBServerName
	OR		HostName = @DBServerName;

-- identify requested database names 
INSERT INTO @Databases (DBName,FileGroup)
SELECT	DISTINCT fs.DatabaseName
		,fs.Filegroup
FROM	dbo.DBFileSizes fs
		INNER JOIN @Servers s ON s.ServerName = fs.ServerName
WHERE	fs.DatabaseName = CASE WHEN @DBName IS NOT NULL THEN @DBName ELSE fs.DatabaseName END;

IF @OmitTempDb = 1
	DELETE
	FROM	@Databases
	WHERE	DBName LIKE '%tempdb%';

-- identify requested report dates
IF @IsSnapshot = 1
BEGIN
	INSERT INTO @t_Drvr (
		ServerName
		,MaxAsOfDate
		,PrevMonthAsOfDate
		,SixMonthAsOfDate
		,TwelveMonthAsOfDate
	)
	SELECT	drvr.ServerName
			,MAX(CASE WHEN drvr.DateRank = 1 THEN drvr.AsOfDate END)
			,MAX(CASE WHEN drvr.DateRank = 2 THEN drvr.AsOfDate END)
			,MAX(CASE WHEN drvr.DateRank = 6 THEN drvr.AsOfDate END)
			,MAX(CASE WHEN drvr.DateRank = 12 THEN drvr.AsOfDate END)
	FROM	(
				SELECT	dt.ServerName
						,dt.AsOfDate
						,RANK() OVER (PARTITION BY dt.ServerName ORDER BY dt.AsOfDate DESC) AS DateRank
				FROM	(
							SELECT	fs.ServerName
									,MIN(CAST(fs.AsOfDate AS SMALLDATETIME)) AS AsOfDate
							FROM	dbo.DBFileSizes fs
									INNER JOIN @Servers s ON s.ServerName = fs.ServerName
							GROUP BY fs.ServerName
									,MONTH(fs.AsOfDate)
									,YEAR(fs.AsOfDate)
						) dt
			) drvr
	GROUP BY drvr.ServerName;

	UPDATE	d
	SET		d.MaxBakDate = b.MaxBakDate
			,d.PrevMonthBakDate = b.PrevMonthBakDate
			,d.SixMonthBakDate = b.SixMonthBakDate
			,d.TwelveMonthBakDate = b.TwelveMonthBakDate
	FROM	@t_Drvr d
			INNER JOIN (
				SELECT	tmp.ServerName
						,MAX(CASE WHEN EOMONTH(tmp.MaxAsOfDate) = EOMONTH(bs.ReportDate) THEN bs.ReportDate END) AS MaxBakDate
						,MAX(CASE WHEN EOMONTH(tmp.PrevMonthAsOfDate) = EOMONTH(bs.ReportDate) THEN bs.ReportDate END) AS PrevMonthBakDate
						,MAX(CASE WHEN EOMONTH(tmp.SixMonthAsOfDate) = EOMONTH(bs.ReportDate) THEN bs.ReportDate END) AS SixMonthBakDate
						,MAX(CASE WHEN EOMONTH(tmp.TwelveMonthAsOfDate) = EOMONTH(bs.ReportDate) THEN bs.ReportDate END) AS TwelveMonthBakDate
				FROM	@t_Drvr tmp
						LEFT OUTER JOIN dbo.DBBakSizes bs ON bs.ServerName = tmp.ServerName
				GROUP BY tmp.ServerName
			) b ON b.ServerName = d.ServerName;

	SELECT	db.ServerName
			,db.DBName AS DatabaseName
			,COALESCE(mr.Filegroup,pv.Filegroup,m6.Filegroup,m12.Filegroup) AS Filegroup
			,COALESCE(mr.FileName,pv.FileName,m6.FileName,m12.FileName) AS FileName
			,mr.Size_MB AS Size_MB
			,mr.Free_MB AS Free_MB
			,mr.PctFree AS PctFree
			,CASE WHEN mr.Size_MB IS NOT NULL AND pv.Size_MB IS NOT NULL THEN mr.Size_MB - pv.Size_MB -- MR & PV exists
				WHEN mr.Size_MB IS NOT NULL AND	pv.Size_MB IS NULL THEN mr.Size_MB -- DB/File added
				WHEN mr.Size_MB IS NULL AND pv.Size_MB IS NOT NULL THEN -1 * pv.Size_MB -- DB/File removed
			END AS PrevMonthGrowth_MB
			,mr.Growth AS AutoGrow_MB
			,mr.VLFs
			,mrb.BackupType
			,(mrb.MostRecentFull_MB + ISNULL(mrb.MostRecentOther_MB,0)) AS BakSize_MB
			,CAST(db.MaxAsOfDate AS DATE) AS ReportedDate
			,pv.Size_MB AS PvSize_MB
			,pvb.BackupType AS PvBackupType
			,(pvb.MostRecentFull_MB + ISNULL(pvb.MostRecentOther_MB,0)) AS PvBakSize_MB
			,CAST(db.PrevMonthAsOfDate AS DATE) AS PvReportedDate
			,m6.Size_MB AS SixMonthSize_MB
			,m6b.BackupType AS SixMonthBackupType
			,(m6b.MostRecentFull_MB + ISNULL(m6b.MostRecentOther_MB,0)) AS SixMonthBakSize_MB
			,CAST(db.SixMonthAsOfDate AS DATE) AS SixMonthReportedDate
			,m12.Size_MB AS TwelveMonthSize_MB
			,m12b.BackupType AS TwelveMonthBackupType
			,(m12b.MostRecentFull_MB + ISNULL(m12b.MostRecentOther_MB,0)) AS TwelveMonthBakSize_MB
			,CAST(db.TwelveMonthAsOfDate AS DATE) AS TwelveMonthReportedDate
	FROM	(
				SELECT	drvr.ServerName
					   ,drvr.MaxAsOfDate
					   ,drvr.PrevMonthAsOfDate
					   ,drvr.SixMonthAsOfDate
					   ,drvr.TwelveMonthAsOfDate
					   ,drvr.MaxBakDate
					   ,drvr.PrevMonthBakDate
					   ,drvr.SixMonthBakDate
					   ,drvr.TwelveMonthBakDate
					   ,d.DBName
					   ,d.FileGroup
				FROM	@t_Drvr drvr
						CROSS JOIN @Databases d
			) db
			LEFT OUTER JOIN dbo.DBFileSizes mr
				ON mr.ServerName = db.ServerName
				AND mr.DatabaseName = db.DBName
				AND mr.Filegroup = db.FileGroup
				AND CAST(mr.AsOfDate AS SMALLDATETIME) = db.MaxAsOfDate
			LEFT OUTER JOIN dbo.DBFileSizes pv
				ON pv.ServerName = db.ServerName
				AND pv.DatabaseName = db.DBName
				AND pv.Filegroup = db.FileGroup
				AND CAST(pv.AsOfDate AS SMALLDATETIME) = db.PrevMonthAsOfDate
			LEFT OUTER JOIN dbo.DBFileSizes m6
				ON m6.ServerName = db.ServerName
				AND m6.DatabaseName = db.DBName
				AND m6.Filegroup = db.FileGroup
				AND CAST(m6.AsOfDate AS SMALLDATETIME) = db.SixMonthAsOfDate
			LEFT OUTER JOIN dbo.DBFileSizes m12
				ON m12.ServerName = db.ServerName
				AND m12.DatabaseName = db.DBName
				AND m12.Filegroup = db.FileGroup
				AND CAST(m12.AsOfDate AS SMALLDATETIME) = db.TwelveMonthAsOfDate
			LEFT OUTER JOIN dbo.DBBakSizes mrb
				ON mrb.ServerName = db.ServerName
				AND mrb.DatabaseName = db.DBName
				AND db.FileGroup = 'PRIMARY'
				AND CAST(mrb.ReportDate AS SMALLDATETIME) = db.MaxBakDate
			LEFT OUTER JOIN dbo.DBBakSizes pvb
				ON pvb.ServerName = db.ServerName
				AND pvb.DatabaseName = db.DBName
				AND db.FileGroup = 'PRIMARY'
				AND CAST(pvb.ReportDate AS SMALLDATETIME) = db.PrevMonthBakDate
			LEFT OUTER JOIN dbo.DBBakSizes m6b
				ON m6b.ServerName = db.ServerName
				AND m6b.DatabaseName = db.DBName
				AND db.FileGroup = 'PRIMARY'
				AND CAST(m6b.ReportDate AS SMALLDATETIME) = db.SixMonthBakDate
			LEFT OUTER JOIN dbo.DBBakSizes m12b
				ON m12b.ServerName = db.ServerName
				AND m12b.DatabaseName = db.DBName
				AND db.FileGroup = 'PRIMARY'
				AND CAST(m12b.ReportDate AS SMALLDATETIME) = db.TwelveMonthBakDate
	WHERE	mr.ServerName IS NOT NULL
	OR		pv.ServerName IS NOT NULL
	OR		m6.ServerName IS NOT NULL
	OR		m12.ServerName IS NOT NULL
	ORDER BY db.ServerName
			,db.DBName
			,COALESCE(mr.FileName,pv.FileName,m6.FileName,m12.FileName);
END;


GO


