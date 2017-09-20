USE [master]
GO

CREATE DATABASE [dba_rep]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'dba_rep', FILENAME = N'D:\sql_data\dba_rep.mdf' , SIZE = 131072KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'dba_rep_log', FILENAME = N'D:\sql_log\dba_rep_log.ldf' , SIZE = 131072KB , MAXSIZE = 2048GB , FILEGROWTH = 131072KB )
GO
ALTER DATABASE [dba_rep] SET COMPATIBILITY_LEVEL = 110
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [dba_rep].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [dba_rep] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [dba_rep] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [dba_rep] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [dba_rep] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [dba_rep] SET ARITHABORT OFF 
GO
ALTER DATABASE [dba_rep] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [dba_rep] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [dba_rep] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [dba_rep] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [dba_rep] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [dba_rep] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [dba_rep] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [dba_rep] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [dba_rep] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [dba_rep] SET  ENABLE_BROKER 
GO
ALTER DATABASE [dba_rep] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [dba_rep] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [dba_rep] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [dba_rep] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [dba_rep] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [dba_rep] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [dba_rep] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [dba_rep] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [dba_rep] SET  MULTI_USER 
GO
ALTER DATABASE [dba_rep] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [dba_rep] SET DB_CHAINING OFF 
GO
ALTER DATABASE [dba_rep] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [dba_rep] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
ALTER DATABASE [dba_rep] SET  READ_WRITE 
GO


USE [dba_rep]
GO

CREATE TABLE [dbo].[ServerList](
	[HostName] [NVARCHAR](128) NOT NULL,
	[Active] [BIT] NOT NULL,
	[Domain] [NVARCHAR](128) NULL,
	[ServerName] [NVARCHAR](128) NOT NULL,
	[Import] [BIT] NOT NULL,
CONSTRAINT [PK_ServerList] PRIMARY KEY CLUSTERED 
(
	[HostName] ASC,
	[ServerName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ServerList] ADD  CONSTRAINT [df_ServerList_Active]  DEFAULT ((1)) FOR [Active]
ALTER TABLE [dbo].[ServerList] ADD  CONSTRAINT [df_ServerList_Import]  DEFAULT ((0)) FOR [Import]
GO

CREATE TABLE [dbo].[DBFileSizes](
	[ServerName] [nvarchar](128) NOT NULL,
	[DatabaseName] [nvarchar](128) NOT NULL,
	[Filegroup] [nvarchar](128) NOT NULL,
	[Size_MB] [int] NOT NULL,
	[Free_MB] [int] NOT NULL,
	[PctFree] [numeric](5, 4) NOT NULL,
	[AsOfDate] [datetime] NOT NULL,
	[VLFs] [int] NULL,
	[FileName] [nvarchar](128) NULL,
	[Growth] [int] NULL,
	[BakFileName] [nvarchar](128) NULL,
	[BakSize] [int] NULL,
 CONSTRAINT [PK_DBFileSizes] UNIQUE CLUSTERED 
(
	[ServerName] ASC,
	[DatabaseName] ASC,
	[Filegroup] ASC,
	[FileName] ASC,
	[AsOfDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[DBBakSizes](
	[ServerName] [nvarchar](128) NOT NULL,
	[DatabaseName] [nvarchar](128) NOT NULL,
	[BackupType] [varchar](20) NOT NULL,
	[UsedCompression] [bit] NULL,
	[UsedChecksum] [bit] NULL,
	[MostRecentFull_Date] [datetime] NULL,
	[MostRecentFull_Sec] [int] NULL,
	[MostRecentFull_MB] [int] NULL,
	[MostRecentOther] [nvarchar](20) NULL,
	[MostRecentOther_Date] [datetime] NULL,
	[MostRecentOther_Sec] [int] NULL,
	[MostRecentOther_MB] [int] NULL,
	[ReportDate] [DATETIME] NOT NULL
 CONSTRAINT [PK_DBBakSizes] UNIQUE CLUSTERED 
(
	[ServerName] ASC,
	[DatabaseName] ASC,
	[ReportDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[SQLAgentJobs](
	[ServerName] [nvarchar](128) NOT NULL,
	[JobName] [nvarchar](128) NOT NULL,
	[JobSteps] [int] NOT NULL,
	[Enabled] [bit] NOT NULL,
	[Schedule] [varchar](100) NOT NULL,
	[OnFailureNotify] [nvarchar](128) NULL,
	[Description] [nvarchar](512) NULL,
	[OutputFile] [nvarchar](200) NULL,
	[AsOfDate] [datetime] NOT NULL,
 CONSTRAINT [PK_SQLAgentJobs] PRIMARY KEY CLUSTERED 
(
	[ServerName] ASC,
	[JobName] ASC,
	[AsOfDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[SQLAgentJobs] ADD  CONSTRAINT [df_SQLAgentJobs_AsOfDate]  DEFAULT (getdate()) FOR [AsOfDate]
GO




USE rp_util;
GO

CREATE TABLE [dbo].[DBBakSizes](
	[DatabaseName] [nvarchar](128) NOT NULL,
	[BackupType] [varchar](20) NOT NULL,
	[UsedCompression] [bit] NULL,
	[UsedChecksum] [bit] NULL,
	[MostRecentFull_Date] [datetime] NULL,
	[MostRecentFull_Sec] [int] NULL,
	[MostRecentFull_MB] [int] NULL,
	[MostRecentOther] [nvarchar](20) NULL,
	[MostRecentOther_Date] [datetime] NULL,
	[MostRecentOther_Sec] [int] NULL,
	[MostRecentOther_MB] [int] NULL,
	[ReportDate] [DATETIME] NOT NULL
 CONSTRAINT [PK_DBBakSizes] UNIQUE CLUSTERED 
(
	[DatabaseName] ASC,
	[ReportDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[DBBakSizes] ADD  CONSTRAINT [DF_DBBakSizes_ReportDate]  DEFAULT (GETDATE()) FOR [ReportDate]
GO
