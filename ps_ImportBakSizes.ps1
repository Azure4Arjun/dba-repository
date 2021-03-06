#Clear screen (for testing)
Clear-Host

#Define target repository
$DBServer = "RPRMBSDEVDB81.REALPOINTDEV.GMACCM.COM"
$DBName = "dba_rep"
$UtilUser = "rp_util_reader"
#$UtilPwd = read-host "Password for" $UtilUser
$UtilPwd = get-content "C:\Users\nateh\Projects\dba-repository\ImportUserPwd.txt"

#SQLCMD timeout parameter
$QueryTimeout = 120

#Get list of servers to import data from
$sql_serverlist="
SELECT	sl.HostName
		,sl.ServerName + '.' + sl.Domain AS ServerName
		,ISNULL(MAX(bs.ReportDate),'1/1/1980') AS ReportDate
FROM	dbo.ServerList sl
		LEFT OUTER JOIN dbo.DBBakSizes bs ON bs.ServerName = sl.HostName
WHERE	sl.Import = 1
AND		sl.Active = 1
GROUP BY sl.HostName
		,sl.ServerName + '.' + sl.Domain;
"

#To Do If Invoke-SqlCmd Is Not Recognized in Windows PowerShell
#Add-PSSnapin SqlServerCmdletSnapin100
#Add-PSSnapin SqlServerProviderSnapin100

$servers = Invoke-Sqlcmd -ServerInstance $DBServer -Database $DBName -Query $sql_serverlist
#$servers | select-object

#Setup DataTable
$dt = new-object Data.DataTable
$col1 = new-object Data.DataColumn ServerName,([string])
$col2 = new-object Data.DataColumn DatabaseName,([string])
$col3 = new-object Data.DataColumn BackupType,([string])
$col4 = new-object Data.DataColumn UsedCompression,([string])
$col5 = new-object Data.DataColumn UsedChecksum,([string])
$col6 = new-object Data.DataColumn MostRecentFull_Date,([string])
$col7 = new-object Data.DataColumn MostRecentFull_Sec,([string])
$col8 = new-object Data.DataColumn MostRecentFull_MB,([string])
$col9 = new-object Data.DataColumn MostRecentOther,([string])
$col10 = new-object Data.DataColumn MostRecentOther_Date,([string])
$col11 = new-object Data.DataColumn MostRecentOther_Sec,([string])
$col12 = new-object Data.DataColumn MostRecentOther_MB,([string])
$col13 = new-object Data.DataColumn ReportDate,([datetime])
$dt.columns.add($col1)
$dt.columns.add($col2)
$dt.columns.add($col3)
$dt.columns.add($col4)
$dt.columns.add($col5)
$dt.columns.add($col6)
$dt.columns.add($col7)
$dt.columns.add($col8)
$dt.columns.add($col9)
$dt.columns.add($col10)
$dt.columns.add($col11)
$dt.columns.add($col12)
$dt.columns.add($col13)

#Loop through servers and pull in bak file data
foreach($server in $servers)
{
  #Retrieve ServerName and MAX(ReportDate) from array
  $hostname = $server[0]
  $servername = $server[1]
  $reportdate = $server[2].ToString()

  #Build SQL to retrieve records for import
  $sql_bakdata="
  SELECT  '$hostname' AS ServerName
         ,DatabaseName
         ,BackupType
         ,ISNULL(CAST(UsedCompression AS VARCHAR(10)),'NULL') AS UsedCompression
         ,ISNULL(CAST(UsedChecksum AS VARCHAR(10)),'NULL') AS UsedChecksum
         ,ISNULL(CAST(MostRecentFull_Date AS VARCHAR(20)),'NULL') AS MostRecentFull_Date
         ,ISNULL(CAST(MostRecentFull_Sec AS VARCHAR(10)),'NULL') AS MostRecentFull_Sec
         ,ISNULL(CAST(MostRecentFull_MB AS VARCHAR(10)),'NULL') AS MostRecentFull_MB
         ,ISNULL(CAST(MostRecentOther AS VARCHAR(50)),'NULL') AS MostRecentOther
         ,ISNULL(CAST(MostRecentOther_Date AS VARCHAR(20)),'NULL') AS MostRecentOther_Date
         ,ISNULL(CAST(MostRecentOther_Sec AS VARCHAR(10)),'NULL') AS MostRecentOther_Sec
         ,ISNULL(CAST(MostRecentOther_MB AS VARCHAR(10)),'NULL') AS MostRecentOther_MB
         ,ReportDate
  FROM	rp_util.dbo.DBBakSizes
  WHERE	CAST(ReportDate AS SMALLDATETIME) > '$reportdate';
  "
  #write-host $sql_bakdata
  
  #Run SQL and capture results in array
  if ($servername -like "*MORNINGSTAR.COM")
  {
    $dt += Invoke-Sqlcmd -ServerInstance $servername -Query $sql_bakdata -QueryTimeout $QueryTimeout -Username $UtilUser -Password $UtilPwd
    #$dt += Invoke-Sqlcmd -ServerInstance $servername -Query $sql_bakdata -QueryTimeout $QueryTimeout -Credential $SQLCredential
  } else
  {
    $dt += Invoke-Sqlcmd -ServerInstance $servername -Query $sql_bakdata -QueryTimeout $QueryTimeout
  }
}
#$dt | select-object

#Load data
$SqlConnection = new-object System.Data.SqlClient.SqlConnection
$SqlConnection.ConnectionString = "Server=$DBServer; Database=$DBName; Integrated Security=SSPI;"
$SqlConnection.Open()
$SqlCommand = new-object System.Data.SqlClient.SqlCommand
$SqlCommand.Connection = $SqlConnection

foreach ($dtrow in $dt)
{
  If ($dtrow.ServerName) #Skip NULL/empty records
  {
    $SqlInsert = "INSERT dbo.DBBakSizes VALUES("
    $SqlInsert += "'$($dtrow.ServerName)','$($dtrow.DatabaseName)','$($dtrow.BackupType)',$($dtrow.UsedCompression),$($dtrow.UsedChecksum)"
    $SqlInsert += if ($dtrow.MostRecentFull_Date -like "NULL") {",NULL"} else {",'$($dtrow.MostRecentFull_Date)'"}
    $SqlInsert += if ($dtrow.MostRecentFull_Sec  -like "NULL") {",NULL"} else {",$($dtrow.MostRecentFull_Sec)"}
    $SqlInsert += if ($dtrow.MostRecentFull_MB  -like "NULL") {",NULL"} else {",$($dtrow.MostRecentFull_MB)"}
    $SqlInsert += if ($dtrow.MostRecentOther -like "NULL") {",NULL"} else {",'$($dtrow.MostRecentOther)'"}
    $SqlInsert += if ($dtrow.MostRecentOther_Date -like "NULL") {",NULL"} else {",'$($dtrow.MostRecentOther_Date)'"}
    $SqlInsert += if ($dtrow.MostRecentOther_Sec  -like "NULL") {",NULL"} else {",$($dtrow.MostRecentOther_Sec)"}
    $SqlInsert += if ($dtrow.MostRecentOther_MB  -like "NULL") {",NULL"} else {",$($dtrow.MostRecentOther_MB)"}
    $SqlInsert += ",'$($dtrow.ReportDate)')" 
    #write-output $SqlInsert
    $SqlCommand.CommandText = $SqlInsert
    $SqlCommand.ExecuteNonQuery()
  }
}

$SqlConnection.Close()
