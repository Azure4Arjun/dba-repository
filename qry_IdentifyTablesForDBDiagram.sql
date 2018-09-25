DECLARE @procname_filter NVARCHAR(128) = NULL;
SET @procname_filter = '%p_pcs%'

DROP TABLE #tmp_SPList;
CREATE TABLE #tmp_SPList (SPName NVARCHAR(128) PRIMARY KEY);

DROP TABLE #tmp_ObjectList;
CREATE TABLE #tmp_ObjectList (objname NVARCHAR(128) PRIMARY KEY, objtype NVARCHAR(60));

-- identify list of SPs in cache
INSERT  #tmp_SPList (SPName)
SELECT  DISTINCT
        SCHEMA_NAME(p.schema_id) + '.' + p.name AS SP
FROM    sys.procedures                              p
        LEFT OUTER JOIN sys.dm_exec_procedure_stats qs
            ON qs.object_id = p.object_id
WHERE   p.is_ms_shipped = 0
AND		(@procname_filter IS NULL
		OR p.name LIKE @procname_filter);

-- first pass at object list used by procs in cache
INSERT  #tmp_ObjectList (objname, objtype)
SELECT  e.referenced_schema_name + '.' + e.referenced_entity_name AS objname
       ,MAX(o.type_desc)                                               AS objtype
FROM    #tmp_SPList                                                      tmp
        CROSS APPLY sys.dm_sql_referenced_entities(tmp.SPName, 'OBJECT') e
        LEFT OUTER JOIN sys.objects o
            ON e.referenced_id = o.object_id
WHERE	e.referenced_entity_name IS NOT NULL
AND		e.referenced_schema_name IS NOT NULL
AND		o.type_desc IS NOT NULL
GROUP BY e.referenced_schema_name + '.' + e.referenced_entity_name;

-- second pass at object list used by procs in cache
INSERT  #tmp_ObjectList (objname, objtype)
SELECT  e.referenced_schema_name + '.' + e.referenced_entity_name AS objname
       ,MAX(o.type_desc)                                          AS objtype
FROM    #tmp_ObjectList                                                   tmp
        CROSS APPLY sys.dm_sql_referenced_entities(tmp.objname, 'OBJECT') e
        LEFT OUTER JOIN sys.objects     o
            ON e.referenced_id = o.object_id
        LEFT OUTER JOIN #tmp_ObjectList ol
            ON tmp.objname = ol.objname
WHERE   tmp.objtype <> 'USER_TABLE'
AND     ol.objname IS NULL
AND     e.referenced_entity_name IS NOT NULL
AND     e.referenced_schema_name IS NOT NULL
AND     o.type_desc IS NOT NULL
GROUP BY e.referenced_schema_name + '.' + e.referenced_entity_name;

SELECT  tmp.objname
       ,tmp.objtype
FROM    #tmp_ObjectList tmp
WHERE   tmp.objtype = 'USER_TABLE'
--OR      tmp.objtype = 'SYNONYM'
ORDER BY tmp.objname;

