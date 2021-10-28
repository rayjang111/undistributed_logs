from pyspark.sql import SparkSession
from hdfs import InsecureClient

hadoop_client = InsecureClient(url=f"http://localhost:9870", user="hadoop")
hadoop_client.delete("touch_sample")
spark = SparkSession\
    .builder\
    .appName("spark_session_example")\
    .getOrCreate()


master_df = spark.read.option("sep","\t").csv("KEPA_Master/sample_master_data.txt", header= True)
uci_df = spark.read.option("sep","\t").csv("KEPA_Master/sample_uci_1000000.txt", header= True)
# master_df.show()
# uci_df.show()

master_df.coalesce(1).write.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/master_df')
uci_df.coalesce(1).write.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/uci_df')
