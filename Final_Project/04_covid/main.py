from fastapi import FastAPI
import pandas as pd 
import sqlalchemy as sa

app = FastAPI()

@app.get("/")
async def root():

	conn_str = 'mysql+pymysql://pipeline:GetWHO2024@corona-db:3306/corona'
	engine = sa.create_engine(conn_str)
	conn = engine.connect()
	hospital = pd.read_sql("covid19", conn)
	conn.close()

	return hospital.to_dict("records")
