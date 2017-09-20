USE [master]
GO

IF OBJECT_ID('dbo.fn_hadr_group_is_primary','FN') IS NOT NULL
OR OBJECT_ID('dbo.fn_hadr_group_is_primary','IF') IS NOT NULL
OR OBJECT_ID('dbo.fn_hadr_group_is_primary','TF') IS NOT NULL
	DROP FUNCTION dbo.fn_hadr_group_is_primary;
GO

CREATE FUNCTION dbo.fn_hadr_group_is_primary (@AGName sysname)
RETURNS BIT
AS
BEGIN
    DECLARE @PrimaryReplica sysname;

	IF @AGName IS NULL
		SET @AGName = @@SERVERNAME;

    SELECT  @PrimaryReplica = hags.primary_replica
    FROM    sys.dm_hadr_availability_group_states hags
            INNER JOIN sys.availability_groups ag ON ag.group_id = hags.group_id;

    IF UPPER(@PrimaryReplica) = UPPER(@@SERVERNAME)
        RETURN 1; -- primary

    RETURN 0; -- not primary
END;

GO


