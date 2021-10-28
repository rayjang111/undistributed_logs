from pyspark.sql import SparkSession
from hdfs import InsecureClient

hadoop_client = InsecureClient(url=f"http://localhost:9870", user="hadoop")
hadoop_client.delete("touch_sample")
spark = SparkSession\
    .builder\
    .appName("spark_session_example")\
    .getOrCreate()


pf_file_count = len(hadoop_client.list("/user/hadoop/공연미분배로그"))
dm_file_count = len(hadoop_client.list("/user/hadoop/디음송미분배로그"))
bs_file_count = len(hadoop_client.list("/user/hadoop/방송미분배로그"))


pf_df = spark.read.option("sep","\t").csv("공연미분배로그/*.txt")
dm_df = spark.read.option("sep","\t").csv("디음송미분배로그/*.txt")
bs_df = spark.read.option("sep","\t").csv("방송미분배로그/*.txt")

pf_df.show()
# pf_df_count = pf_df.count()
# dm_df_count = dm_df.count()
# bs_df_count = bs_df.count()

pf_df = pf_df.select(pf_df.columns[3:6])
dm_df = dm_df.select(dm_df.columns[3:6])
bs_df = bs_df.select(bs_df.columns[3:6])

###
column_names = ['album','title', 'singer']
pf_df = pf_df.toDF(*column_names)
dm_df = dm_df.toDF(*column_names)
bs_df = bs_df.toDF(*column_names)

pf_df.coalesce(1).write.parquet('pf_df')
dm_df.coalesce(1).write.parquet('dm_df')
bs_df.coalesce(1).write.parquet('bs_df')
