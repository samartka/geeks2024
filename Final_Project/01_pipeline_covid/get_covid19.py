# Daily COVID-19 cases and deaths by date reported to WHO. https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-data.csv
# https://data.who.int/dashboards/covid19/data?n=o
# Copyright : Attribution 4.0 International (CC BY 4.0)
# Citation : World Health Organization 2023 data.who.int, WHO Coronavirus (COVID-19) dashboard > Data [Dashboard]. https://data.who.int/dashboards/covid19/data

import requests
import pandas as pd
import sqlalchemy as sa
import os
import os.path
from io import StringIO
from datetime import datetime
use_na_values= ['-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A N/A', '#N/A', 'N/A', 'n/a', '', '#NA', 'NULL', 'null', 'NaN', '-NaN', 'nan', '-nan', '']


def notifyLine(s):
  token = "bET4TmYKRWJg7UQLtXEI8uD8IyFt7ASE5XexbQv3WK6"
  headers = {'Authorization': "Bearer {}".format(token)}

  url = 'https://notify-api.line.me/api/notify'

  response = requests.post(
      url, headers=headers, params={"message":s}
  )


def getCovidInfo(covid_df):
    covid_info = [covid_df["Date_reported"].count(),covid_df["Date_reported"].max(),covid_df["New_cases"].sum(),covid_df["New_deaths"].sum()]
    return pd.DataFrame([covid_info],columns=['rows','latest','total_cases','total_deaths'])

def isNewerCovid(loaded_df):
    loaded_df_info = getCovidInfo(loaded_df)
    if os.path.isfile(os.path.join(os.getcwd(),'inf\cov.inf')):
        prev_df = pd.read_csv(os.path.join(os.getcwd(),'inf\cov.inf'))
        if loaded_df_info.iloc[0]['rows'] > prev_df.iloc[0]['rows'] : 
            return True
        elif loaded_df_info.iloc[0]['latest'] > prev_df.iloc[0]['latest'] :
            return True
        elif loaded_df_info.iloc[0]['total_cases'] > prev_df.iloc[0]['total_cases'] :
            return True
        elif loaded_df_info.iloc[0]['total_deaths'] > prev_df.iloc[0]['total_deaths'] :
            return True
        else:
            return False
    else:
        return True

def writeCovidInfo(loaded_df):
    getCovidInfo(loaded_df).to_csv(os.path.join(os.getcwd(),'inf\cov.inf'),index=False)
    


covid_csv = requests.get('https://srhdpeuwpubsa.blob.core.windows.net/whdh/COVID/WHO-COVID-19-global-data.csv')
decoded_covid_csv = StringIO(covid_csv.content.decode('utf-8'))

df_covid = pd.read_csv(decoded_covid_csv,keep_default_na=False,na_values=use_na_values,dtype={'New_cases': 'Int64','New_deaths': 'Int64'})
if isNewerCovid(df_covid):
    #Transform
    df_covid[['New_cases_fill', 'New_deaths_fill']] = df_covid[['New_cases','New_deaths']].fillna(value=0)
    df_covid[['WHO_region']] = df_covid[['WHO_region']].fillna(value='Not specify')
    #Load to csv
    df_covid.to_csv(os.path.join(os.getcwd(),'result\covid_cases_deaths.csv'),index=False)
    #Load to local database
    
    conn_str = f"mysql+pymysql://pipeline:GetWHO2024@corona-db:3306/corona"
    engine = sa.create_engine(conn_str)
    conn = engine.connect()
    df_covid.to_sql("covid19", conn, index=None, if_exists="replace")
    conn.close()
    
    #Set current COVID-19 data info
    writeCovidInfo(df_covid)
    notifyLine(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}|COVID-19|Found new data and loaded successfully')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}|COVID-19|Found new data and loaded successfully')    
else:
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}|COVID-19|Current data is up to date')

