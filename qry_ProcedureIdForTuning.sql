USE rp_util;

DECLARE @DBName        NVARCHAR(128) = 'rp_prod'
       ,@PerfThreshold INT           --= 3000 -- 3 seconds
       ,@TopN          INT           = 10
       ,@SQLRestart    DATETIME;

-- Last Restart of SQL Server --
SELECT  @SQLRestart = sqlserver_start_time
FROM    sys.dm_os_sys_info;

SELECT  'Last restart: ' + CONVERT(VARCHAR(50), @SQLRestart, 100);

DECLARE @SPModified TABLE (
    [Database]  NVARCHAR(128)
   ,SP          NVARCHAR(128)
   ,SPModified  DATE
   ,CaptureDate DATETIME
);

IF @PerfThreshold IS NULL
BEGIN
	SELECT  'SPs with no logged executions';

	SELECT DISTINCT
			p1.[Database]
		   ,p1.SP
	FROM    dbo.ProcedureBenchmark                 p1
			LEFT OUTER JOIN dbo.ProcedureBenchmark p2
				ON  p2.[Database] = p1.[Database]
				AND p2.SP = p1.SP
				AND p2.ExecutionCount IS NOT NULL
	WHERE   p1.ExecutionCount IS NULL
	AND     p2.ProcedureBenchmarkId IS NULL
	AND     p1.[Database] = CASE WHEN @DBName IS NOT NULL THEN @DBName
								 ELSE p1.[Database]
							END
	ORDER BY p1.[Database]
			,p1.SP;

	SELECT  'SPs with modifications';

	INSERT INTO @SPModified ([Database], SP, SPModified, CaptureDate)
	SELECT  p.[Database]
		   ,p.SP
		   ,p.SPModified
		   ,MAX(p.CaptureDate)
	FROM    dbo.ProcedureBenchmark p
			INNER JOIN
			(
				SELECT  [Database]
					   ,SP
				FROM    dbo.ProcedureBenchmark
				WHERE  [Database] = CASE WHEN @DBName IS NOT NULL THEN @DBName
										 ELSE [Database]
									END
				AND    ExecutionCount IS NOT NULL
				GROUP BY [Database]
						,SP
				HAVING COUNT(DISTINCT SPModified) > 1
			)                      x
				ON  x.[Database] = p.[Database]
				AND x.SP = p.SP
	GROUP BY p.[Database]
			,p.SP
			,p.SPModified;

	SELECT  p.[Database]
		   ,p.SP
		   ,p.SPModified
		   ,CAST(p.CaptureDate AS DATE) AS CaptureDate
		   ,p.MinutesInCache -- Time (in minutes) since the stored procedure was added to the cache.
		   ,p.LastExecution -- Last time at which the stored procedure was executed.
		   ,p.ExecutionCount -- Number of times the stored procedure has been executed since it was last compiled.
		   ,p.AvgElapsedTime / 1000     AS AvgElapsedTime -- Average elapsed time, in milliseconds, for completed executions of this stored procedure.
		   ,p.[Calls/Second] -- Average number of times per second the stored procedure has been executed since it was compiled.
		   ,p.MaxCPUTime / 1000         AS MaxCPUTime -- Maximum CPU time, in milliseconds, that this stored procedure has ever consumed during a single execution.
		   ,p.AvgCPUTime / 1000         AS AvgCPUTime -- Average amount of CPU time, in milliseconds, that was consumed by executions of this stored procedure since it was compiled.
		   ,p.MaxPhysicalReads -- Maximum number of physical reads that this stored procedure has ever performed during a single execution.
		   ,p.AvgPhysicalReads -- Average number of physical reads performed by executions of this stored procedure since it was compiled.
		   ,p.MaxLogicalReads -- Maximum number of logical reads that this stored procedure has ever performed during a single execution.
		   ,p.AvgLogicalReads -- Average number of logical reads performed by executions of this stored procedure since it was compiled.
		   ,p.MaxLogicalWrites -- Maximum number of logical writes that this stored procedure has ever performed during a single execution.
		   ,p.AvgLogicalWrites -- Average number of logical writes performed by executions of this stored procedure since it was compiled.
		   ,p.[LogicalWrites/Min] -- Average number of writes per minute performed by executions of this stored procedure since it was compiled.
	FROM    @SPModified                       tmp
			INNER JOIN dbo.ProcedureBenchmark p
				ON  p.[Database] = tmp.[Database]
				AND p.SP = tmp.SP
				AND p.SPModified = tmp.SPModified
				AND p.CaptureDate = tmp.CaptureDate
	ORDER BY tmp.[Database]
			,tmp.SP
			,DENSE_RANK() OVER (PARTITION BY tmp.[Database], tmp.SP ORDER BY tmp.SPModified DESC);

	DELETE  FROM @SPModified;
END;

INSERT INTO @SPModified ([Database], SP, CaptureDate)
SELECT DISTINCT
		[Database]
		,SP
		,MAX(CaptureDate)
FROM    dbo.ProcedureBenchmark
WHERE   [Database] = CASE WHEN @DBName IS NOT NULL THEN @DBName
							ELSE [Database]
						END
GROUP BY [Database]
		,SP;

SELECT  'Top ' + CAST(@TopN AS VARCHAR(10)) + ' CPU consumers';

SELECT TOP (@TopN)
		p.[Database]
	   ,p.SP
	   ,p.SPModified
	   ,CAST(p.CaptureDate AS DATE) AS CaptureDate
	   ,p.MinutesInCache -- Time (in minutes) since the stored procedure was added to the cache.
	   ,p.LastExecution -- Last time at which the stored procedure was executed.
	   ,p.ExecutionCount -- Number of times the stored procedure has been executed since it was last compiled.
	   ,p.AvgElapsedTime / 1000     AS AvgElapsedTime -- Average elapsed time, in milliseconds, for completed executions of this stored procedure.
	   ,p.[Calls/Second] -- Average number of times per second the stored procedure has been executed since it was compiled.
	   ,p.MaxCPUTime / 1000         AS MaxCPUTime -- Maximum CPU time, in milliseconds, that this stored procedure has ever consumed during a single execution.
	   ,p.AvgCPUTime / 1000         AS AvgCPUTime -- Average amount of CPU time, in milliseconds, that was consumed by executions of this stored procedure since it was compiled.
	   ,p.MaxPhysicalReads -- Maximum number of physical reads that this stored procedure has ever performed during a single execution.
	   ,p.AvgPhysicalReads -- Average number of physical reads performed by executions of this stored procedure since it was compiled.
	   ,p.MaxLogicalReads -- Maximum number of logical reads that this stored procedure has ever performed during a single execution.
	   ,p.AvgLogicalReads -- Average number of logical reads performed by executions of this stored procedure since it was compiled.
	   ,p.MaxLogicalWrites -- Maximum number of logical writes that this stored procedure has ever performed during a single execution.
	   ,p.AvgLogicalWrites -- Average number of logical writes performed by executions of this stored procedure since it was compiled.
	   ,p.[LogicalWrites/Min] -- Average number of writes per minute performed by executions of this stored procedure since it was compiled.
FROM    @SPModified                       tmp
        INNER JOIN dbo.ProcedureBenchmark p
            ON  p.[Database] = tmp.[Database]
            AND p.SP = tmp.SP
            AND p.CaptureDate = tmp.CaptureDate
WHERE   p.AvgElapsedTime / 1000 >= COALESCE(@PerfThreshold, p.AvgElapsedTime / 1000)
ORDER BY p.TotalCPUTime DESC
        ,tmp.[Database]
        ,tmp.SP
        ,tmp.SPModified DESC;
		
SELECT  'Top ' + CAST(@TopN AS VARCHAR(10)) + ' IO consumers';

SELECT TOP (@TopN)
		p.[Database]
	   ,p.SP
	   ,p.SPModified
	   ,CAST(p.CaptureDate AS DATE) AS CaptureDate
	   ,p.MinutesInCache -- Time (in minutes) since the stored procedure was added to the cache.
	   ,p.LastExecution -- Last time at which the stored procedure was executed.
	   ,p.ExecutionCount -- Number of times the stored procedure has been executed since it was last compiled.
	   ,p.AvgElapsedTime / 1000     AS AvgElapsedTime -- Average elapsed time, in milliseconds, for completed executions of this stored procedure.
	   ,p.[Calls/Second] -- Average number of times per second the stored procedure has been executed since it was compiled.
	   ,p.MaxCPUTime / 1000         AS MaxCPUTime -- Maximum CPU time, in milliseconds, that this stored procedure has ever consumed during a single execution.
	   ,p.AvgCPUTime / 1000         AS AvgCPUTime -- Average amount of CPU time, in milliseconds, that was consumed by executions of this stored procedure since it was compiled.
	   ,p.MaxPhysicalReads -- Maximum number of physical reads that this stored procedure has ever performed during a single execution.
	   ,p.AvgPhysicalReads -- Average number of physical reads performed by executions of this stored procedure since it was compiled.
	   ,p.MaxLogicalReads -- Maximum number of logical reads that this stored procedure has ever performed during a single execution.
	   ,p.AvgLogicalReads -- Average number of logical reads performed by executions of this stored procedure since it was compiled.
	   ,p.MaxLogicalWrites -- Maximum number of logical writes that this stored procedure has ever performed during a single execution.
	   ,p.AvgLogicalWrites -- Average number of logical writes performed by executions of this stored procedure since it was compiled.
	   ,p.[LogicalWrites/Min] -- Average number of writes per minute performed by executions of this stored procedure since it was compiled.
FROM    @SPModified                       tmp
        INNER JOIN dbo.ProcedureBenchmark p
            ON  p.[Database] = tmp.[Database]
            AND p.SP = tmp.SP
            AND p.CaptureDate = tmp.CaptureDate
WHERE   p.AvgElapsedTime / 1000 >= COALESCE(@PerfThreshold, p.AvgElapsedTime / 1000)
ORDER BY (p.TotalPhysicalReads + p.TotalLogicalReads + p.TotalLogicalWrites) DESC
        ,tmp.[Database]
        ,tmp.SP
        ,tmp.SPModified DESC;

IF @PerfThreshold IS NULL
    SELECT  'Most recent logged activity';
ELSE
    SELECT  'SPs with duration over ' + CAST(@PerfThreshold / 1000 AS VARCHAR(10)) + ' seconds';

SELECT  p.[Database]
       ,p.SP
       ,p.SPModified
       ,CAST(p.CaptureDate AS DATE) AS CaptureDate
       ,p.MinutesInCache -- Time (in minutes) since the stored procedure was added to the cache.
       ,p.LastExecution -- Last time at which the stored procedure was executed.
       ,p.ExecutionCount -- Number of times the stored procedure has been executed since it was last compiled.
       ,p.AvgElapsedTime / 1000     AS AvgElapsedTime -- Average elapsed time, in milliseconds, for completed executions of this stored procedure.
       ,p.[Calls/Second] -- Average number of times per second the stored procedure has been executed since it was compiled.
       ,p.MaxCPUTime / 1000         AS MaxCPUTime -- Maximum CPU time, in milliseconds, that this stored procedure has ever consumed during a single execution.
       ,p.AvgCPUTime / 1000         AS AvgCPUTime -- Average amount of CPU time, in milliseconds, that was consumed by executions of this stored procedure since it was compiled.
       ,p.MaxPhysicalReads -- Maximum number of physical reads that this stored procedure has ever performed during a single execution.
       ,p.AvgPhysicalReads -- Average number of physical reads performed by executions of this stored procedure since it was compiled.
       ,p.MaxLogicalReads -- Maximum number of logical reads that this stored procedure has ever performed during a single execution.
       ,p.AvgLogicalReads -- Average number of logical reads performed by executions of this stored procedure since it was compiled.
       ,p.MaxLogicalWrites -- Maximum number of logical writes that this stored procedure has ever performed during a single execution.
       ,p.AvgLogicalWrites -- Average number of logical writes performed by executions of this stored procedure since it was compiled.
       ,p.[LogicalWrites/Min] -- Average number of writes per minute performed by executions of this stored procedure since it was compiled.
FROM    @SPModified                       tmp
        INNER JOIN dbo.ProcedureBenchmark p
            ON  p.[Database] = tmp.[Database]
            AND p.SP = tmp.SP
            AND p.CaptureDate = tmp.CaptureDate
WHERE   p.AvgElapsedTime / 1000 >= COALESCE(@PerfThreshold, p.AvgElapsedTime / 1000)
ORDER BY tmp.[Database]
        ,tmp.SP
        ,tmp.SPModified DESC;


