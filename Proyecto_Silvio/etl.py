import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import NVARCHAR, DECIMAL, INTEGER, DATE
import logging

logging.basicConfig(filename='etl.log', level=logging.INFO,
 format='%(asctime)s:%(levelname)s:%(message)s')

    
#Conexión a la base de datos
try:
    server='localhost'
    database='Covid19Stage'
    driver='ODBC Driver 17 for SQL Server'
    connection_string = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"

    engine=create_engine(connection_string)

    with open('BaseCovid19Select.sql', 'r') as f:
        query=f.read()
    
    data=pd.read_sql(query, engine)
    logging.info('Data loaded successfully')
    print("Datos cargados al DF")
    print(data)
    
    print("DATOS SUCIOS")
    print(data['Outcome'].head())
    print(data['Week End'].head())
    print(data['Age-Adjusted Unvaccinated Rate'].head())
    print(data['Age-Adjusted Vaccinated Rate'].head())
    print(data['Age-Adjusted Vaccinated Ratio'].head())
except Exception as e:
    logging.error(f'Error loading data: {e}')
    raise

#Limpieza de datos y transformaciones iniciales
try:
    data.replace('', pd.NA, inplace=True)
    
    text_columns = ['Outcome', 'Age Group']
    for column in text_columns:
        data[column] = data[column].str.upper()
        data[column].fillna('N/S', inplace=True)

    number_columns = ['Unvaccinated Rate', 'Vaccinated Rate', 'Boosted Rate', 'Crude Vaccinated Ratio',
                     'Crude Boosted Ratio', 'Age-Adjusted Unvaccinated Rate',
                     'Age-Adjusted Vaccinated Rate', 'Age-Adjusted Boosted Rate',
                     'Age-Adjusted Vaccinated Ratio', 'Age-Adjusted Boosted Ratio',
                     'Population Unvaccinated', 'Population Vaccinated',
                     'Population Boosted', 'Outcome Unvaccinated', 'Outcome Vaccinated',
                     'Outcome Boosted', 'Age Group Min', 'Age Group Max']
    for column in number_columns:
        data[column].fillna('0', inplace=True)
    print("DATOS CON LIMPIEZA INICIAL")
    print(data['Outcome'].head())
    print(data['Week End'].head())
    print(data['Age-Adjusted Unvaccinated Rate'].head())
    print(data['Age-Adjusted Vaccinated Rate'].head())
    print(data['Age-Adjusted Vaccinated Ratio'].head())
    
    logging.info('Success filling missing values')
except Exception as e:
    logging.error(f'Error filling missing values: {e}')
    raise

#Transformación y asignación de tipos de datos
try:
    data['Outcome'] = data['Outcome'].astype(str)
    data['Week End'] = pd.to_datetime(data['Week End'])
    data['Day'] = data['Week End'].dt.day
    data['Month'] = data['Week End'].dt.month
    data['Year'] = data['Week End'].dt.year
    data['Age Group'] = data['Age Group'].astype(str)
    data['Unvaccinated Rate'] = data['Unvaccinated Rate'].astype(float)
    data['Vaccinated Rate'] = data['Vaccinated Rate'].astype(float)
    data['Boosted Rate'] = data['Boosted Rate'].astype(float)
    data['Crude Vaccinated Ratio'] = data['Crude Vaccinated Ratio'].astype(float)
    data['Crude Boosted Ratio'] = data['Crude Boosted Ratio'].astype(float)
    data['Age-Adjusted Unvaccinated Rate'] = data['Age-Adjusted Unvaccinated Rate'].astype(float)
    data['Age-Adjusted Vaccinated Rate'] = data['Age-Adjusted Vaccinated Rate'].astype(float)
    data['Age-Adjusted Boosted Rate'] = data['Age-Adjusted Boosted Rate'].astype(float)
    data['Age-Adjusted Vaccinated Ratio'] = data['Age-Adjusted Vaccinated Ratio'].astype(float)
    data['Age-Adjusted Boosted Ratio'] = data['Age-Adjusted Boosted Ratio'].astype(float)
    data['Population Unvaccinated'] = data['Population Unvaccinated'].astype(int)
    data['Population Vaccinated'] = data['Population Vaccinated'].astype(int)
    data['Population Boosted'] = data['Population Boosted'].astype(int)
    data['Outcome Unvaccinated'] = data['Outcome Unvaccinated'].astype(int)
    data['Outcome Vaccinated'] = data['Outcome Vaccinated'].astype(int)
    data['Outcome Boosted'] = data['Outcome Boosted'].astype(int)
    data['Age Group Min'] = data['Age Group Min'].astype(int)
    data['Age Group Max'] = data['Age Group Max'].astype(int)
    logging.info('Data type transformations applied')
except Exception as e:
    logging.error(f'Error applying  data type transformations: {e}')
    raise

#Transformación de datos calculados
try:
     calculated_columns = ['Crude Vaccinated Ratio', 'Crude Boosted Ratio',
                          'Age-Adjusted Vaccinated Ratio', 'Age-Adjusted Boosted Ratio']
    
     def calculate_ratio(row, calculated_column):
        if calculated_column=='Crude Vaccinated Ratio' and row['Vaccinated Rate'] != 0 and row['Unvaccinated Rate'] != 0:
            return row['Unvaccinated Rate'] / row['Vaccinated Rate']
        if calculated_column=='Crude Boosted Ratio' and row['Boosted Rate'] != 0 and row['Unvaccinated Rate'] != 0:
            return row['Unvaccinated Rate'] / row['Boosted Rate']
        if calculated_column=='Age-Adjusted Vaccinated Ratio' and row['Age-Adjusted Vaccinated Rate'] != 0 and row['Age-Adjusted Unvaccinated Rate'] != 0:
            return row['Age-Adjusted Unvaccinated Rate'] / row['Age-Adjusted Vaccinated Rate']
        if calculated_column=='Age-Adjusted Boosted Ratio' and row['Age-Adjusted Boosted Rate'] != 0 and row['Age-Adjusted Unvaccinated Rate'] != 0:
            return row['Age-Adjusted Unvaccinated Rate'] / row['Age-Adjusted Boosted Rate']
        else:
            return 0
        
     for column in calculated_columns:
            data[column] = data.apply(lambda row: calculate_ratio(row, column), axis=1)   
     print("DATOS LUEGO DE CALCULOS")
     print(data['Outcome'].head())
     print(data['Week End'].head())
     print(data['Day'].head())
     print(data['Month'].head())
     print(data['Year'].head())
     print(data['Age-Adjusted Unvaccinated Rate'].head())
     print(data['Age-Adjusted Vaccinated Rate'].head())
     print(data['Age-Adjusted Vaccinated Ratio'].head())
      
     logging.info('Success calculating ratios')
except Exception as e:
    logging.error(f'Error calculating ratios: {e}')
    raise   

#Borrar datos en tabla transformada
try:
    with engine.connect() as conn:
        with open('BaseCovid19Delete.sql', 'r') as f:
            querydelete = f.read()
            conn.execute(text(querydelete))
            conn.commit()
    logging.info('Data deleted successfully')
except Exception as e:
    logging.error(f'Error deleting data: {e}')
    raise

#Asignar tipo de datos para la base e insercion a tabla transformada
try:
    columntypes = {
        'Outcome': NVARCHAR(50),
        'Week End': DATE,
        'Day': INTEGER(),
        'Month': INTEGER(),
        'Year': INTEGER(),
        'Age Group': NVARCHAR(50),
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
        'Population Unvaccinated': INTEGER(),
        'Population Vaccinated': INTEGER(),
        'Population Boosted': INTEGER(),
        'Outcome Unvaccinated': INTEGER(),
        'Outcome Vaccinated': INTEGER(),
        'Outcome Boosted': INTEGER(),
        'Age Group Min': INTEGER(),
        'Age Group Max': INTEGER()
    }
    data.to_sql('TransformedStagingCovid19', engine, if_exists='append', index=False, dtype=columntypes,chunksize=1000)
    logging.info('Data saved successfully to database')
except Exception as e:
    logging.error(f'Error saving data to database: {e}')
    raise
    


    





