#https://www.who.int/teams/global-influenza-programme/surveillance-and-monitoring/influenza-surveillance-outputs
#Permissions and licensing
#Permission from WHO is not required for the use of WHO materials issued under the Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Intergovernmental Organization (CC BY-NC-SA 3.0 IGO) licence.
# Flu ID dataset (VIW_FID) https://xmart-api-public.who.int/FLUMART/VIW_FID?$format=csv  approx. 200MB

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

def getFluIDInfo(flu_id_df):
    fluID_info = [flu_id_df["ISOYW"].count(),flu_id_df["ISOYW"].max()]
    return pd.DataFrame([fluID_info],columns=['rows','latest_isoYW'])

def isNewerFluID(loaded_df):
    loaded_df_info = getFluIDInfo(loaded_df)
    if os.path.isfile(os.path.join(os.getcwd(),'inf\\fluid.inf')):
        prev_df = pd.read_csv(os.path.join(os.getcwd(),'inf\\fluid.inf'))
        if loaded_df_info.iloc[0]['rows'] > prev_df.iloc[0]['rows'] : 
            return True
        elif loaded_df_info.iloc[0]['latest_isoYW'] > prev_df.iloc[0]['latest_isoYW'] :
            return True
        else:
            return False
    else:
        return True

def writeFluIDInfo(loaded_df):
    getFluIDInfo(loaded_df).to_csv(os.path.join(os.getcwd(),'inf\\fluid.inf'),index=False)

flu_id_csv = requests.get('https://xmart-api-public.who.int/FLUMART/VIW_FID?$format=csv')
decoded_flu_id_csv = StringIO(flu_id_csv.content.decode('utf-8'))
df_flu_id = pd.read_csv(decoded_flu_id_csv,keep_default_na=False,na_values=use_na_values)

if isNewerFluID(df_flu_id):
    #Transform

    #Load to csv
    df_flu_id.to_csv(os.path.join(os.getcwd(),'result\VIW_FID.csv'),index=False)
    
    #Load to local database
    
    conn_str = f"mysql+pymysql://pipeline:GetWHO2024@corona-db:3306/corona"
    engine = sa.create_engine(conn_str)
    conn = engine.connect()
    df_flu_id.to_sql("flu_id", conn, index=None, if_exists="replace")
    conn.close()
    #Set current FLU NET data info
    writeFluIDInfo(df_flu_id)
    notifyLine(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}|FLU_ID|Found new data and loaded successfully')
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}|FLU_ID|Found new data and loaded successfully')
else:
    print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}|FLU_ID|Current data is up to date')

