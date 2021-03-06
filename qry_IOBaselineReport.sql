USE rp_util;
GO

DECLARE @MaxCaptureDate DATETIME;

SELECT  @MaxCaptureDate = MAX(CaptureDate)
FROM    dbo.IOLatency;

IF OBJECT_ID('TempDb.dbo.#tmp_DataSet', 'U') IS NOT NULL
    DROP TABLE #tmp_DataSet;

CREATE TABLE #tmp_DataSet (
    ReadLatency       BIGINT
   ,WriteLatency      BIGINT
   ,Latency           BIGINT
   ,[Database]        NVARCHAR(128)
   ,FileNameExtension CHAR(3)
   ,FilePath          CHAR(3)
   ,CaptureDate       DATETIME
   ,CaptureMonth      INT
   ,CaptureYear       INT
   ,MostRecent        BIT
);

WITH DBList AS (
    SELECT  DISTINCT
            [Database]
    FROM    dbo.IOLatency
    WHERE   CaptureDate = @MaxCaptureDate
)
INSERT  #tmp_DataSet (
     ReadLatency
    ,WriteLatency
    ,Latency
    ,[Database]
    ,FileNameExtension
    ,FilePath
    ,CaptureDate
    ,CaptureMonth
    ,CaptureYear
	,MostRecent
)
SELECT  l.ReadLatency
       ,l.WriteLatency
       ,l.Latency
       ,l.[Database]
       ,RIGHT(l.Filename, 3) AS FileNameExtension
       ,LEFT(l.Path, 3)      AS FilePath
       ,l.CaptureDate
       ,MONTH(l.CaptureDate) AS CaptureMonth
       ,YEAR(l.CaptureDate)  AS CaptureYear
	   ,CASE WHEN l.CaptureDate = @MaxCaptureDate THEN 1 ELSE 0 END
FROM    DBList                   dl
        INNER JOIN dbo.IOLatency l
            ON dl.[Database] = l.[Database]
WHERE   l.CaptureDate > DATEADD(YEAR, -1, @MaxCaptureDate);

-- yearly average
SELECT  FilePath
       ,AVG(ReadLatency)                                                                        AS AvgReadLatency
       ,CONVERT(VARCHAR(50), MIN(ReadLatency)) + '-' + CONVERT(VARCHAR(50), MAX(ReadLatency))   AS ReadLatency
       ,AVG(WriteLatency)                                                                       AS AvgWriteLatency
       ,CONVERT(VARCHAR(50), MIN(WriteLatency)) + '-' + CONVERT(VARCHAR(50), MAX(WriteLatency)) AS WriteLatency
       ,AVG(Latency)                                                                            AS AvgLatency
       ,CONVERT(VARCHAR(50), MIN(Latency)) + '-' + CONVERT(VARCHAR(50), MAX(Latency))           AS Latency
FROM    #tmp_DataSet
GROUP BY FilePath
ORDER BY FilePath;

-- monthly baseline
WITH RolledUpDS AS (
    SELECT  CaptureMonth
           ,CaptureYear
           ,FilePath
           ,AVG(ReadLatency)                                                 AS ReadLatency
           ,AVG(WriteLatency)                                                AS WriteLatency
           ,AVG(Latency)                                                     AS Latency
           ,DATENAME(MONTH, CaptureDate) + ' ' + DATENAME(YEAR, CaptureDate) AS CaptureLabel
    FROM    #tmp_DataSet
    GROUP BY CaptureMonth
            ,CaptureYear
            ,FilePath
            ,DATENAME(MONTH, CaptureDate) + ' ' + DATENAME(YEAR, CaptureDate)
)
,MRDS AS (
    SELECT  CaptureMonth + 1                       AS CaptureMonth -- adding +1 to Month & Year so it shows up as rightmost column in pivot
           ,CaptureYear + 1                        AS CaptureYear
           ,FilePath
           ,AVG(ReadLatency)                       AS ReadLatency
           ,AVG(WriteLatency)                      AS WriteLatency
           ,AVG(Latency)                           AS Latency
           ,CONVERT(VARCHAR(10), CaptureDate, 101) AS CaptureLabel
    FROM    #tmp_DataSet
    WHERE   MostRecent = 1
    GROUP BY CaptureMonth
            ,CaptureYear
            ,FilePath
            ,CONVERT(VARCHAR(10), CaptureDate, 101)
)
,ColHeaders AS (
SELECT  x.CaptureLabel
       ,DENSE_RANK() OVER (ORDER BY x.CaptureYear, x.CaptureMonth) AS SortOrder
FROM    (
            SELECT  RolledUpDS.CaptureMonth
                   ,RolledUpDS.CaptureYear
                   ,RolledUpDS.CaptureLabel
            FROM    RolledUpDS
            UNION
            SELECT  MRDS.CaptureMonth
                   ,MRDS.CaptureYear
                   ,MRDS.CaptureLabel
            FROM    MRDS
        ) x
)

SELECT  1                                                   AS SortOrder
       ,'FilePath'                                          AS Col0 
       ,MAX(CASE WHEN SortOrder = 1 THEN CaptureLabel END)  AS Col1
       ,MAX(CASE WHEN SortOrder = 2 THEN CaptureLabel END)  AS Col2
       ,MAX(CASE WHEN SortOrder = 3 THEN CaptureLabel END)  AS Col3
       ,MAX(CASE WHEN SortOrder = 4 THEN CaptureLabel END)  AS Col4
       ,MAX(CASE WHEN SortOrder = 5 THEN CaptureLabel END)  AS Col5
       ,MAX(CASE WHEN SortOrder = 6 THEN CaptureLabel END)  AS Col6
       ,MAX(CASE WHEN SortOrder = 7 THEN CaptureLabel END)  AS Col7
       ,MAX(CASE WHEN SortOrder = 8 THEN CaptureLabel END)  AS Col8
       ,MAX(CASE WHEN SortOrder = 9 THEN CaptureLabel END)  AS Col9
       ,MAX(CASE WHEN SortOrder = 10 THEN CaptureLabel END) AS Col0
       ,MAX(CASE WHEN SortOrder = 11 THEN CaptureLabel END) AS Col1
       ,MAX(CASE WHEN SortOrder = 12 THEN CaptureLabel END) AS Col2
       ,MAX(CASE WHEN SortOrder = 13 THEN CaptureLabel END) AS Col3
FROM    ColHeaders
UNION ALL
SELECT  2                                                   AS SortOrder
       ,COALESCE(r.FilePath,mr.FilePath)                                                             AS Col0
       ,MAX(CASE WHEN h.SortOrder = 1 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col1
       ,MAX(CASE WHEN h.SortOrder = 2 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col2
       ,MAX(CASE WHEN h.SortOrder = 3 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col3
       ,MAX(CASE WHEN h.SortOrder = 4 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col4
       ,MAX(CASE WHEN h.SortOrder = 5 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col5
       ,MAX(CASE WHEN h.SortOrder = 6 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col6
       ,MAX(CASE WHEN h.SortOrder = 7 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col7
       ,MAX(CASE WHEN h.SortOrder = 8 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col8
       ,MAX(CASE WHEN h.SortOrder = 9 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END)  AS Col9
       ,MAX(CASE WHEN h.SortOrder = 10 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END) AS Col0
       ,MAX(CASE WHEN h.SortOrder = 11 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END) AS Col1
       ,MAX(CASE WHEN h.SortOrder = 12 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END) AS Col2
       ,MAX(CASE WHEN h.SortOrder = 13 THEN CONVERT(VARCHAR(50),COALESCE(r.Latency,mr.Latency)) END) AS Col3
FROM	ColHeaders h
		LEFT OUTER JOIN RolledUpDS r
			ON h.CaptureLabel = r.CaptureLabel
		LEFT OUTER JOIN MRDS mr
			ON h.CaptureLabel = mr.CaptureLabel
GROUP BY COALESCE(r.FilePath,mr.FilePath)
ORDER BY 1, 2

