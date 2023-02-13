import pyspark
sparkql = pyspark.sql.SparkSession.builder.master('local').getOrCreate()

import os
data_dir = './dsa-airflow/data'

file_names = ['AAPL','ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM','INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA']

dataframes = {}

for csv in file_names:
    df = sparkql.read.csv(os.path.join(data_dir,csv+'.csv'), header=True)
    dataframes[csv] = df

print(dataframes)