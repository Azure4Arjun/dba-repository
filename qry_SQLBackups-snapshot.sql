USE dba_rep;
GO

DECLARE @ServerName sysname = '';

WITH BackupDate AS (
    SELECT  ServerName
           ,MAX(ReportDate) AS ReportDate
    FROM    dbo.DBBakSizes
    WHERE   ServerName = @ServerName
    OR      @ServerName IS NULL
    GROUP BY ServerName
)
,DBBackup AS (
    SELECT  bs.ServerName
           ,bs.DatabaseName
           ,bs.BackupType
           ,bs.UsedCompression
           ,bs.UsedChecksum
           ,bs.MostRecentFull_Date
           ,bs.MostRecentFull_Sec
           ,bs.MostRecentFull_MB
           ,bs.MostRecentOther
           ,bs.MostRecentOther_Date
           ,bs.MostRecentOther_Sec
           ,bs.MostRecentOther_MB
           ,bs.ReportDate
           ,RANK() OVER (PARTITION BY bs.ServerName, bs.DatabaseName ORDER BY bs.ReportDate DESC) AS DateOrder
    FROM    BackupDate                d
            INNER JOIN dbo.DBBakSizes bs
                ON d.ServerName = bs.ServerName
)
,AvgBackup AS (
    SELECT  b.ServerName
           ,b.DatabaseName
           ,b.BackupType
           ,b.UsedCompression
           ,b.UsedChecksum
           ,AVG(b.MostRecentFull_Sec)  AS AvgMostRecentFull_Sec
           ,AVG(b.MostRecentFull_MB)   AS AvgMostRecentFull_MB
           ,b.MostRecentOther
           ,AVG(b.MostRecentOther_Sec) AS AvgMostRecentOther_Sec
           ,AVG(b.MostRecentOther_MB)  AS AvgMostRecentOther_MB
    FROM    DBBackup b
    WHERE   b.DateOrder BETWEEN 1 AND 6
    GROUP BY b.ServerName
            ,b.DatabaseName
            ,b.BackupType
            ,b.UsedCompression
            ,b.UsedChecksum
            ,b.MostRecentOther
)
SELECT  b.ServerName
       ,b.DatabaseName
       ,b.BackupType
       ,b.UsedCompression
       ,b.UsedChecksum
       ,b.MostRecentFull_Date
       ,b.MostRecentFull_Sec
       ,b.MostRecentFull_MB
       ,a.AvgMostRecentFull_Sec  AS AvgLastSixFull_Sec
       ,a.AvgMostRecentFull_MB   AS AvgLastSixFull_MB
       ,b.MostRecentOther
       ,b.MostRecentOther_Date
       ,b.MostRecentOther_Sec
       ,b.MostRecentOther_MB
       ,a.AvgMostRecentOther_Sec AS AvgLastSixOther_Sec
       ,a.AvgMostRecentOther_MB  AS AvgLastSixOther_MB
FROM    DBBackup                  b
		INNER JOIN BackupDate     d
			ON b.ServerName = d.ServerName
			AND b.ReportDate = d.ReportDate
        LEFT OUTER JOIN AvgBackup a
            ON  b.ServerName = a.ServerName
            AND b.DatabaseName = a.DatabaseName
            AND b.BackupType = a.BackupType
            AND b.UsedCompression = a.UsedCompression
            AND b.UsedChecksum = a.UsedChecksum
            AND ISNULL(b.MostRecentOther, 'NA') = ISNULL(a.MostRecentOther, 'NA')
WHERE   b.DateOrder = 1;