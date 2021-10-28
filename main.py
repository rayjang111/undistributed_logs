from pyspark.sql import SparkSession
from hdfs import InsecureClient

hadoop_client = InsecureClient(url=f"http://localhost:9870", user="hadoop")
hadoop_client.delete("touch_sample")
spark = SparkSession\
    .builder\
    .appName("spark_session_example")\
    .getOrCreate()

pf_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/pf_df/*.parquet')
dm_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/dm_df/*.parquet')
bs_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/bs_df/*.parquet')
master_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/master_df/*.parquet')
uci_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/uci_df/*.parquet')