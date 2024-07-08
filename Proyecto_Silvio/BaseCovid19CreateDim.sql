USE MASTER;

IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'Covid19Dimensional')
BEGIN
CREATE DATABASE Covid19Dimensional;
END 
GO

IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'Covid19Dimensional')
BEGIN
USE Covid19Dimensional;

DROP TABLE IF EXISTS dbo.FactCovid19Report;
DROP TABLE IF EXISTS dbo.DimDate;
DROP TABLE IF EXISTS dbo.DimOutcome;
DROP TABLE IF EXISTS dbo.DimAgeGroup;


CREATE TABLE [dbo].[DimDate](
	[DateID] DATE NOT NULL,
	[Day] INT NOT NULL,
	[Month] INT NOT NULL,
	[Year] INT NOT NULL,
	CONSTRAINT [PK_DFecha] PRIMARY KEY ([DateID]),
) 

CREATE TABLE [dbo].[DimOutcome](
	[OutcomeID] INT,
	[Outcome] [NVARCHAR] (50) NOT NULL,
	CONSTRAINT [PK_DOutcome] PRIMARY KEY ([OutcomeID])
) 



CREATE TABLE [dbo].[DimAgeGroup](
	[AgeGroupID] INT,
	[Age Group] [NVARCHAR] (50) NOT NULL,
	CONSTRAINT [PK_DAgeGroup] PRIMARY KEY ([AgeGroupID])
) 



CREATE TABLE [dbo].[FactCovid19Report](
	[ReportID] INT,

	[Unvaccinated Rate] DECIMAL(10,2) NOT NULL,
	[Vaccinated Rate] DECIMAL(10,2) NOT NULL,
	[Boosted Rate] DECIMAL(10,2) NOT NULL,
	[Crude Vaccinated Ratio] DECIMAL(10,2) NOT NULL,
	[Crude Boosted Ratio] DECIMAL(10,2) NOT NULL,

	[Age-Adjusted Unvaccinated Rate] DECIMAL(10,2) NOT NULL,
	[Age-Adjusted Vaccinated Rate] DECIMAL(10,2) NOT NULL,
	[Age-Adjusted Boosted Rate] DECIMAL(10,2) NOT NULL,
	[Age-Adjusted Vaccinated Ratio] DECIMAL(10,2) NOT NULL,
	[Age-Adjusted Boosted Ratio] DECIMAL(10,2) NOT NULL,
	
	[Population Unvaccinated] INT NOT NULL,
	[Population Vaccinated] INT NOT NULL,
	[Population Boosted] INT NOT NULL,

	[Outcome Unvaccinated] INT NOT NULL,
	[Outcome Vaccinated] INT NOT NULL,
	[Outcome Boosted] INT NOT NULL,
	
	[DateID] DATE NOT NULL,
	[OutcomeID] INT NOT NULL,
	[AgeGroupID] INT  NOT NULL,

	CONSTRAINT [PK_FCovid19Report] PRIMARY KEY ([ReportID]),
	CONSTRAINT [dateid_FK_fact] FOREIGN KEY ([DateID]) REFERENCES DimDate([DateID]),
	CONSTRAINT [outcomeid_FK_fact] FOREIGN KEY ([OutcomeID]) REFERENCES DimOutcome([OutcomeID]),
	CONSTRAINT [agegroupid_FK_fact] FOREIGN KEY ([AgeGroupID]) REFERENCES DimAgeGroup([AgeGroupID]),

	
) 



END
GO