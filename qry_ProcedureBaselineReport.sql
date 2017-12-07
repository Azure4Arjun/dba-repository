USE rp_util;

DECLARE @DBName      NVARCHAR(128) = 'cmbs_ni'
       ,@SPName      NVARCHAR(128) = ''
       ,@ShowHistory BIT           = 0
       ,@SQLRestart  DATETIME;

-- Last Restart of SQL Server --
SELECT  @SQLRestart = sqlserver_start_time
FROM    sys.dm_os_sys_info;

SELECT  'Last restart: ' + CONVERT(VARCHAR(50), @SQLRestart, 100);

WITH ModDates AS (
    SELECT	DISTINCT
			SP
		   ,SPModified                                                   AS ModDate
		   ,DENSE_RANK() OVER (PARTITION BY SP ORDER BY SPModified DESC) AS ModRank
    FROM    dbo.ProcedureBenchmark
    WHERE   [Database] = @DBName
    AND     (ISNULL(@SPName, '') = '' OR @SPName = SP)
)

SELECT  x.SP
       ,x.LastModDate
       ,x.LastExec
       ,x.AvgExecCount
       ,CASE WHEN x.mnAvgElapsedTime = x.mxAvgElapsedTime THEN CAST(x.mnAvgElapsedTime AS VARCHAR(25))
             ELSE CAST(x.mnAvgElapsedTime AS VARCHAR(25)) + '-' + CAST(x.mxAvgElapsedTime AS VARCHAR(25))
        END AS AvgDuration_ms
       ,CASE WHEN x.mnAvgCPUTime = x.mxAvgCPUTime THEN CAST(x.mnAvgCPUTime AS VARCHAR(25))
             ELSE CAST(x.mnAvgCPUTime AS VARCHAR(25)) + '-' + CAST(x.mxAvgCPUTime AS VARCHAR(25))
        END AS AvgCPUTime_ms
       ,CASE WHEN x.mnAvgPhysicalReads = x.mxAvgPhysicalReads THEN CAST(x.mnAvgPhysicalReads AS VARCHAR(25))
             ELSE CAST(x.mnAvgPhysicalReads AS VARCHAR(25)) + '-' + CAST(x.mxAvgPhysicalReads AS VARCHAR(25))
        END AS AvgPhysicalReads
       ,CASE WHEN x.mnAvgLogicalReads = x.mxAvgLogicalReads THEN CAST(x.mnAvgLogicalReads AS VARCHAR(25))
             ELSE CAST(x.mnAvgLogicalReads AS VARCHAR(25)) + '-' + CAST(x.mxAvgLogicalReads AS VARCHAR(25))
        END AS AvgLogicalReads
       ,CASE WHEN x.mnAvgLogicalWrites = x.mxAvgLogicalWrites THEN CAST(x.mnAvgLogicalWrites AS VARCHAR(25))
             ELSE CAST(x.mnAvgLogicalWrites AS VARCHAR(25)) + '-' + CAST(x.mxAvgLogicalWrites AS VARCHAR(25))
        END AS AvgLogicalWrites
FROM	(
			SELECT  b.SP
				   --,cte.ModRank
				   ,cte.ModDate                  AS LastModDate
				   ,MAX(b.LastExecution)         AS LastExec
				   ,AVG(b.ExecutionCount)        AS AvgExecCount
				   ,MIN(b.AvgElapsedTime / 1000) AS mnAvgElapsedTime
				   ,MAX(b.AvgElapsedTime / 1000) AS mxAvgElapsedTime
				   ,MIN(b.AvgCPUTime / 1000)     AS mnAvgCPUTime
				   ,MAX(b.AvgCPUTime / 1000)     AS mxAvgCPUTime
				   ,MIN(b.AvgPhysicalReads)      AS mnAvgPhysicalReads
				   ,MAX(b.AvgPhysicalReads)      AS mxAvgPhysicalReads
				   ,MIN(b.AvgLogicalReads)       AS mnAvgLogicalReads
				   ,MAX(b.AvgLogicalReads)       AS mxAvgLogicalReads
				   ,MIN(b.AvgLogicalWrites)      AS mnAvgLogicalWrites
				   ,MAX(b.AvgLogicalWrites)      AS mxAvgLogicalWrites
			FROM    ModDates                          cte
					INNER JOIN dbo.ProcedureBenchmark b
						ON  b.[Database] = @DBName
						AND cte.SP = b.SP
						AND cte.ModDate = b.SPModified
			WHERE	@ShowHistory = 1 OR cte.ModRank = 1
			GROUP BY b.SP
					--,cte.ModRank
					,cte.ModDate
		) x
ORDER BY x.SP
		,x.LastModDate DESC;

