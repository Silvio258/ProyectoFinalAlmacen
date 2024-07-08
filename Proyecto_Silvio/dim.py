import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, DECIMAL, INTEGER, DATE
import logging

logging.basicConfig(filename='etl.log', level=logging.INFO,
 format='%(asctime)s:%(levelname)s:%(message)s')

#CONEXIONES Y EXTRACCION DE DIMENSIONES A UN DF
try:
    server='localhost'
    driver='ODBC Driver 17 for SQL Server'
    
    database_origin='Covid19Stage'
    connection_string_origin = f"mssql+pyodbc://@{server}/{database_origin}?driver={driver}&trusted_connection=yes"
    engine_origin=create_engine(connection_string_origin)
    
    database_destiny='Covid19Dimensional'
    connection_string_destiny= f"mssql+pyodbc://@{server}/{database_destiny}?driver={driver}&trusted_connection=yes"

    engine_destiny=create_engine(connection_string_destiny)

    #DATE
    with open('ExtractDimDate.sql', 'r') as d:
        extract_dim_date=d.read()
    
    dim_date=pd.read_sql(extract_dim_date, engine_origin)
    
    #OUTCOME
    with open('ExtractDimOutcome.sql', 'r') as o:
        extract_dim_outcome=o.read()
        
    dim_outcome=pd.read_sql(extract_dim_outcome, engine_origin)
    dim_outcome.index = dim_outcome.index + 1
    dim_outcome.index.name = 'OutcomeID'
    #AGE GROUP
    with open('ExtractDimAgeGroup.sql', 'r') as a:
        extract_dim_age=a.read()
        
    dim_age=pd.read_sql(extract_dim_age, engine_origin)
    dim_age.index = dim_age.index + 1
    dim_age.index.name = 'AgeGroupID'
    print(dim_date)
    print(dim_outcome)
    print(dim_age)
    
    logging.info('Dim dataframes loaded successfully')
except Exception as e:
    logging.error(f'Error loading dim dataframes: {e}')
    raise

#DELETE DE DATOS 
try:
    with engine_destiny.connect() as conn:
        with open('BaseCovid19DimDelete.sql', 'r') as d:
            querydeletedim = d.read()
            conn.execute(text(querydeletedim))
            conn.commit()
    logging.info('Dimensional data deleted successfully')
except Exception as e:
    logging.error(f'Error deleting dimensional data: {e}')
    raise

#CARGA A DIM DATE, DIM OUTCOME Y DIM AGE
try:
    date_columns = {
        'DateID': DATE,
        'Year': INTEGER,
        'Month': INTEGER,
        'Day': INTEGER,
    }
    dim_date.to_sql('DimDate', engine_destiny, if_exists='append', index=False, dtype=date_columns)
    logging.info('Dates loaded successfully')
except Exception as e:
    logging.error(f'Error loading dates: {e}')
    raise

try:
    outcome_columns = {
        'Outcome': NVARCHAR(50)
    }
    dim_outcome.to_sql('DimOutcome', engine_destiny, if_exists='append', index=True, dtype=outcome_columns)
    logging.info('Outcome loaded successfully')
except Exception as e:
    logging.error(f'Error loading outcomes: {e}')
    raise

try:
    age_columns = {
        'Age Group': NVARCHAR(50)
    }
    dim_age.to_sql('DimAgeGroup', engine_destiny, if_exists='append', index=True, dtype=age_columns)
    logging.info('Ages loaded successfully')
except Exception as e:
    logging.error(f'Error loading ages: {e}')
    raise

#EXTRACCION DE FACT REPORT A UN DF
try:
    #COVID19REPORT
    with open('ExtractFactReport.sql', 'r') as r:
        extract_fact_report=r.read()
        
    fact_report=pd.read_sql(extract_fact_report, engine_origin)
    print(fact_report)
    fact_report.index = fact_report.index + 1
    fact_report.index.name = 'ReportID'
    logging.info('Fact dataframes loaded successfully')
except Exception as e:
    logging.error(f'Error saving fact dataframe: {e}')
    raise

#CARGA A FACT REPORT
try:
    report_columns = {
        'Unvaccinated Rate': DECIMAL(10, 2),
        'Vaccinated Rate': DECIMAL(10, 2),
        'Boosted Rate': DECIMAL(10, 2),
        'Crude Vaccinated Ratio': DECIMAL(10, 2),
        'Crude Boosted Ratio': DECIMAL(10, 2),
        'Age-Adjusted Unvaccinated Rate': DECIMAL(10, 2),
        'Age-Adjusted Vaccinated Rate': DECIMAL(10, 2),
        'Age-Adjusted Boosted Rate': DECIMAL(10, 2),
        'Age-Adjusted Vaccinated Ratio': DECIMAL(10, 2),
        'Age-Adjusted Boosted Ratio': DECIMAL(10, 2),
        'Population Unvaccinated': INTEGER,
        'Population Vaccinated': INTEGER,
        'Population Boosted': INTEGER,
        'Outcome Unvaccinated': INTEGER,
        'Outcome Vaccinated': INTEGER,
        'Outcome Boosted': INTEGER,
        'DateID': DATE,
        'OutcomeID': INTEGER,
        'AgeGroupID': INTEGER
    }
    fact_report.to_sql('FactCovid19Report', engine_destiny, if_exists='append', index=True, dtype=report_columns)
    logging.info('reports loaded successfully')
except Exception as e:
    logging.error(f'Error loading reports: {e}')
    raise

