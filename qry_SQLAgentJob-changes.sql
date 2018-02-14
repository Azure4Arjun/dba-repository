USE dba_rep;

DECLARE @ServerName      sysname = ''
       ,@ShowOnlyChanges BIT     = 1;

WITH SQLJobs AS (
    SELECT  j.ServerName
           ,j.JobName
           ,j.JobSteps
           ,j.Enabled
           ,j.Schedule
           ,j.OnFailureNotify
           ,j.Description
           ,j.OutputFile
           ,j.AsOfDate
           ,CAST(j.AsOfDate AS DATE) AS HistDate
           ,h.HistOrder
    FROM    dbo.SQLAgentJobs j
            INNER JOIN
            (
                SELECT  ServerName
                       ,JobName
                       ,AsOfDate
                       ,ROW_NUMBER() OVER (PARTITION BY ServerName, JobName ORDER BY AsOfDate DESC) AS HistOrder
                FROM    dbo.SQLAgentJobs
                WHERE  ServerName = @ServerName
                OR     NULLIF(@ServerName, '') IS NULL
            )                h
                ON  h.ServerName = j.ServerName
                AND h.JobName = j.JobName
                AND h.AsOfDate = j.AsOfDate
    WHERE   h.HistOrder IN (1, 2)
)
SELECT  CASE WHEN sj1.HistOrder = 1
             AND  sj1.HistDate <> hd.HistDate THEN 'Y'
             ELSE ''
        END AS DELETED
       ,sj1.ServerName
       ,sj1.JobName
       ,sj1.JobSteps
       ,sj1.Enabled
       ,sj1.Schedule
       ,sj1.OnFailureNotify
       ,sj1.Description
       ,sj1.OutputFile
       ,sj1.AsOfDate
FROM    SQLJobs                 sj1
        INNER JOIN
        (
            SELECT  SQLJobs.ServerName
                   ,MAX(SQLJobs.HistDate) AS HistDate
            FROM    SQLJobs
            GROUP BY SQLJobs.ServerName
        )                       hd
            ON sj1.ServerName = hd.ServerName
        LEFT OUTER JOIN SQLJobs sj2
            ON  sj1.ServerName = sj2.ServerName
            AND sj1.JobName = sj2.JobName
            AND sj1.HistOrder + 1 = sj2.HistOrder
WHERE   sj1.HistOrder = 1
AND     @ShowOnlyChanges = 0
UNION ALL
SELECT  CASE WHEN sj1.HistDate <> md.MRHistDate THEN 'Y'
             ELSE ''
        END AS DELETED
       ,sj1.ServerName
       ,sj1.JobName
       ,sj1.JobSteps
       ,sj1.Enabled
       ,sj1.Schedule
       ,sj1.OnFailureNotify
       ,sj1.Description
       ,sj1.OutputFile
       ,sj1.AsOfDate
FROM    SQLJobs                 sj1
        INNER JOIN
        (
            SELECT  SQLJobs.ServerName
                   ,MAX(SQLJobs.HistDate) AS HistDate
            FROM    SQLJobs
            GROUP BY SQLJobs.ServerName
        )                       hd
            ON sj1.ServerName = hd.ServerName
        LEFT OUTER JOIN SQLJobs sj2
            ON  sj1.ServerName = sj2.ServerName
            AND sj1.JobName = sj2.JobName
            AND sj1.HistOrder + 1 = sj2.HistOrder
        CROSS JOIN
        (
            SELECT  MAX(SQLJobs.HistDate) AS MRHistDate
            FROM    SQLJobs
            WHERE  @ShowOnlyChanges = 1
        )                       md
WHERE   sj1.HistOrder = 1
AND     @ShowOnlyChanges = 1
AND     (sj1.HistDate = md.MRHistDate OR sj2.HistDate = md.MRHistDate)
AND     (
            sj2.ServerName IS NULL
      OR    (
                sj2.ServerName IS NOT NULL
         AND    (
                    sj1.JobSteps <> sj2.JobSteps
              OR    sj1.Enabled <> sj2.Enabled
              OR    sj1.Schedule <> sj2.Schedule
              OR    sj1.OnFailureNotify <> sj2.OnFailureNotify
              OR    sj1.Description <> sj2.Description
              OR    sj1.OutputFile <> sj2.OutputFile
                )
            )
        )
ORDER BY sj1.ServerName
        ,1 DESC
        ,sj1.JobName;

