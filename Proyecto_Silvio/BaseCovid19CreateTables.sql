USE MASTER;

IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'Covid19Stage')
BEGIN
CREATE DATABASE Covid19Stage;
END 
GO

IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'Covid19Stage')
BEGIN
USE Covid19Stage;

DROP TABLE IF EXISTS [dbo].[TransformedStagingCovid19];

CREATE TABLE [dbo].[TransformedStagingCovid19](
	[Outcome] [NVARCHAR] (50) NOT NULL,
	[Week End] DATE NOT NULL,
	[Year] INT NOT NULL,
	[Month] INT NOT NULL,
	[Day] INT NOT NULL,
	[Age Group] [NVARCHAR] (50) NOT NULL,
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
	[Age Group Min] INT NOT NULL,
	[Age Group Max] INT NOT NULL
) 
END
GO

