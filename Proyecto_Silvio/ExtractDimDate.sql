SELECT DISTINCT
T.[Week End] as DateID,
T.Year,
T.Month,
T.Day
FROM [dbo].[TransformedStagingCovid19] T
ORDER BY T.[Week End] ASC
