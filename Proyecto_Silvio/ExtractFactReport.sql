SELECT DISTINCT
T.[Unvaccinated Rate],
T.[Vaccinated Rate],
T.[Boosted Rate],
T.[Crude Vaccinated Ratio],
T.[Crude Boosted Ratio],
T.[Age-Adjusted Unvaccinated Rate],
T.[Age-Adjusted Vaccinated Rate],
T.[Age-Adjusted Boosted Rate],
T.[Age-Adjusted Vaccinated Ratio],
T.[Age-Adjusted Boosted Ratio],
T.[Population Unvaccinated],
T.[Population Vaccinated],
T.[Population Boosted],
T.[Outcome Unvaccinated],
T.[Outcome Vaccinated],
T.[Outcome Boosted],
T.[Week End] as [DateID],
CASE 
    WHEN T.Outcome = 'HOSPITALIZATIONS' THEN 1
    WHEN T.Outcome = 'CASES' THEN 2
    WHEN T.Outcome = 'DEATHS' THEN 3
END AS OutcomeID,
CASE 
    WHEN T.[Age Group] = '0-4' THEN 1
    WHEN T.[Age Group] = '12-17' THEN 2
    WHEN T.[Age Group] = '18-29' THEN 3
    WHEN T.[Age Group] = '30-49' THEN 4
    WHEN T.[Age Group] = '50-64' THEN 5
    WHEN T.[Age Group] = '5-11' THEN 6
    WHEN T.[Age Group] = '65-79' THEN 7
    WHEN T.[Age Group] = '80+' THEN 8
    WHEN T.[Age Group] = 'ALL' THEN 9
END AS AgeGroupID
FROM [dbo].[TransformedStagingCovid19] T