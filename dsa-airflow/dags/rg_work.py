import pyspark
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf      # sf = spark functions
import pyspark.sql.types as st          # st = spark types


sparkql = pyspark.sql.SparkSession.builder.master('local').getOrCreate()

import os
data_dir = './dsa-airflow/data'

file_names = ['AAPL','ADBE','AMZN', 'CRM', 'CSCO', 'GOOGL', 'IBM','INTC','META','MSFT','NFLX','NVDA','ORCL','TSLA']

dataframes = {}

for csv in file_names:
    df = sparkql.read.csv(os.path.join(data_dir,csv+'.csv'), header=True)
    df = df.withColumn('stock_name', sf.lit(csv))
    df.toDF('date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'stock_name')
    df.select('stock_name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume')

print(dataframes)