from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from hdfs import InsecureClient
from pyspark.sql.functions import expr, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

hadoop_client = InsecureClient(url=f"http://localhost:9870", user="hadoop")
hadoop_client.delete("touch_sample")
spark = SparkSession\
    .builder\
    .appName("spark_session_example")\
    .config("spark.driver.memory","3g")\
    .getOrCreate()

pf_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/pf_df/*.parquet')
dm_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/dm_df/*.parquet')
bs_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/bs_df/*.parquet')
master_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/master_df/*.parquet')
uci_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/uci_df/*.parquet')

# total_data = dm_df.count()
# dm_df = dm_df.select('*',monotonically_increasing_id())
# dm_df.printSchema()
#
# count_included_df = spark.sparkContext.emptyRDD()
# schema = StructType([StructField('monotonically_increasing_id()',LongType(),True)])
# count_included_df = spark.createDataFrame(count_included_df, schema)

# ----------------------------------------------pf_df part----------------------------------------------
print("----------------------------------------------pf_df part----------------------------------------------")
###singer, album 이 겹치는 것들
unique_master = master_df.select(col('SINGER_NM'),col('ALBUM_NM')).distinct()
new_df = pf_df.join(unique_master,(pf_df['singer']==unique_master['SINGER_NM'])& (pf_df['album']==unique_master['ALBUM_NM']), "inner")
count_n = new_df.count() ###singer 이 겹치는 데이터의 수
print("pf_df_singer_album_included :", count_n)
# count_included_df.count()

#singer, title 이 겹치는것들
unique_master = master_df.select(col('SINGER_NM'),col('CONTENT_NM')).distinct()
new_df = pf_df.join(unique_master,(pf_df['singer']==unique_master['SINGER_NM'])&(pf_df['title']==unique_master['CONTENT_NM']), "inner")
count_n =new_df.count() ### album 이 겹치는 데이터의 수
print("pf_df_singer_title_included :", count_n)
# count_included_df.count()

#title, album이 겹치는것들
unique_master = master_df.select(col('CONTENT_NM'),col('ALBUM_NM')).distinct()
new_df = pf_df.join(unique_master,(pf_df['title']==unique_master['CONTENT_NM'])& (pf_df['album']==unique_master['ALBUM_NM']), "inner")
count_n = new_df.count() ###title 이 겹치는 데이터의 수
print("pf_df_album_title_included : ", count_n)
# count_included_df.count()

###셋다 겹치는것들:
unique_master = master_df.select(col('SINGER_NM'),col('CONTENT_NM'), col('ALBUM_NM')).distinct()
new_df = pf_df.join(unique_master,(pf_df['singer']==unique_master['SINGER_NM'])&
                    (pf_df['album']==unique_master['ALBUM_NM']) &
                    (pf_df['title']==unique_master['CONTENT_NM']), "inner")
count_n =new_df.count() ### album 이 겹치는 데이터의 수
print("pf_df_all_included :", count_n)
# count_included_df.count()


#---------------------------------------------- dm_df part----------------------------------------------
print("----------------------------------------------dm_df part----------------------------------------------")
###singer, album 이 겹치는 것들
unique_master = master_df.select(col('SINGER_NM'),col('ALBUM_NM')).distinct()
new_df = dm_df.join(unique_master,(dm_df['singer']==unique_master['SINGER_NM'])& (dm_df['album']==unique_master['ALBUM_NM']), "inner")
count_n = new_df.count() ###singer 이 겹치는 데이터의 수
print("dm_df_singer_album_included :", count_n)
# count_included_df.count()

#singer, title 이 겹치는것들
unique_master = master_df.select(col('SINGER_NM'),col('CONTENT_NM')).distinct()
new_df = dm_df.join(unique_master,(dm_df['singer']==unique_master['SINGER_NM'])&(dm_df['title']==unique_master['CONTENT_NM']), "inner")
count_n =new_df.count() ### album 이 겹치는 데이터의 수
print("dm_df_singer_title_included :", count_n)
# count_included_df.count()

#title, album이 겹치는것들
unique_master = master_df.select(col('CONTENT_NM'),col('ALBUM_NM')).distinct()
new_df = dm_df.join(unique_master,(dm_df['title']==unique_master['CONTENT_NM'])& (dm_df['album']==unique_master['ALBUM_NM']), "inner")
count_n = new_df.count() ###title 이 겹치는 데이터의 수
print("dm_df_album_title_included : ", count_n)
# count_included_df.count()

###셋다 겹치는것들:
unique_master = master_df.select(col('SINGER_NM'),col('CONTENT_NM'), col('ALBUM_NM')).distinct()
new_df = dm_df.join(unique_master,(dm_df['singer']==unique_master['SINGER_NM'])&
                    (dm_df['album']==unique_master['ALBUM_NM']) &
                    (dm_df['title']==unique_master['CONTENT_NM']), "inner")
count_n =new_df.count() ### album 이 겹치는 데이터의 수
print("dm_df_all_included :", count_n)
# count_included_df.count()


#---------------------------------------------- bs_df part----------------------------------------------
print("----------------------------------------------bs_df part----------------------------------------------")
###singer, album 이 겹치는 것들
unique_master = master_df.select(col('SINGER_NM'),col('ALBUM_NM')).distinct()
new_df = bs_df.join(unique_master,(bs_df['singer']==unique_master['SINGER_NM'])& (bs_df['album']==unique_master['ALBUM_NM']), "inner")
count_n = new_df.count() ###singer 이 겹치는 데이터의 수
print("bs_df_singer_album_included :", count_n)
# count_included_df.count()

#singer, title 이 겹치는것들
unique_master = master_df.select(col('SINGER_NM'),col('CONTENT_NM')).distinct()
new_df = bs_df.join(unique_master,(bs_df['singer']==unique_master['SINGER_NM'])&(bs_df['title']==unique_master['CONTENT_NM']), "inner")
count_n =new_df.count() ### album 이 겹치는 데이터의 수
print("bs_df_singer_title_included :", count_n)
# count_included_df.count()

#title, album이 겹치는것들
unique_master = master_df.select(col('CONTENT_NM'),col('ALBUM_NM')).distinct()
new_df = bs_df.join(unique_master,(bs_df['title']==unique_master['CONTENT_NM'])& (bs_df['album']==unique_master['ALBUM_NM']), "inner")
count_n = new_df.count() ###title 이 겹치는 데이터의 수
print("bs_df_album_title_included : ", count_n)
# count_included_df.count()

###셋다 겹치는것들:
unique_master = master_df.select(col('SINGER_NM'),col('CONTENT_NM'), col('ALBUM_NM')).distinct()
new_df = bs_df.join(unique_master,(bs_df['singer']==unique_master['SINGER_NM'])&
                    (bs_df['album']==unique_master['ALBUM_NM']) &
                    (bs_df['title']==unique_master['CONTENT_NM']), "inner")
count_n =new_df.count() ### album 이 겹치는 데이터의 수
print("bs_df_all_included :", count_n)
# count_included_df.count()

