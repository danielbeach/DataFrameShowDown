from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import logging

spark = SparkSession.builder.appName('testing') \
        .enableHiveSupport().getOrCreate()
logging.basicConfig(format='%(asctime)s %(levelname)s - Testing - %(message)s'
                        , level=logging.ERROR)
d1 = datetime.now()
df = spark.read.csv('data/*.csv', header='True')
df = df.withColumn('started_at', F.col('started_at').cast('date'))
df = df.groupby(F.month(F.col('started_at'))).agg(F.count('*'))
print(df.show())
d2 = datetime.now()
print(d2-d1)
