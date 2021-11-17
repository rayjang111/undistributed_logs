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
    .config("spark.driver.memory","2g")\
    .getOrCreate()

pf_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/pf_df/*.parquet')
dm_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/dm_df/*.parquet')
bs_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/bs_df/*.parquet')
master_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/master_df/*.parquet')
uci_df = spark.read.parquet('hdfs://hadoop-spark-1:9000/user/hadoop/uci_df/*.parquet')

###기본 데이터 조작
#공연미분배
# pf_df.printSchema()
# pf_df.count()
# pf_df.where(col("album").isNull()).count()
# pf_df.where(col("title").isNull()).count()
# pf_df.where(col("singer").isNull()).count()
# pf_df.where(col("album").isNull()).where(col("title").isNull()).count()
# pf_df.where(col("album").isNull()).where(col("singer").isNull()).count()
# pf_df.where(col("title").isNull()).where(col("singer").isNull()).count()
#
# pf_df.where(col("album").isNull()).where(col("singer").isNull()).show()
#
# #디음송미분배
# dm_df.printSchema()
# dm_df.count()
# dm_df.where(col("album").isNull()).count()
# dm_df.where(col("title").isNull()).count()
# dm_df.where(col("singer").isNull()).count()
# dm_df.where(col("album").isNull()).where(col("title").isNull()).count()
# dm_df.where(col("album").isNull()).where(col("singer").isNull()).count()
# dm_df.where(col("title").isNull()).where(col("singer").isNull()).count()
# dm_df.where(col("title").isNull()).where(col("singer").isNull()).where(col("album").isNull()).count()
# #방송미분배
# bs_df.printSchema()
# bs_df.count()
# bs_df.where(col("album").isNull()).count()
# bs_df.where(col("title").isNull()).count()
# bs_df.where(col("singer").isNull()).count()
# bs_df.where(col("album").isNull()).where(col("title").isNull()).count()
# bs_df.where(col("album").isNull()).where(col("singer").isNull()).count()
# bs_df.where(col("title").isNull()).where(col("singer").isNull()).count() # memory 1g 일땐 에러가 나옴, where절을 한개만 씀으로서 메모리 사용량을 줄일 수 있다.
# bs_df.where(col("title").isNull()).where(col("singer").isNull()).where(col("album").isNull()).count()
#
# ###각 요소별 최빈값 파악
# #공연미분배
#
# pf_df.groupby(col('album')).count().sort(col('count').desc()).show()
# pf_df.groupby(col('title')).count().sort(col('count').desc()).show()
# pf_df.groupby(col('singer')).count().sort(col('count').desc()).show()
#
# #디음송미분배
#
# dm_df.groupby(col('album')).count().sort(col('count').desc()).show()
# dm_df.groupby(col('title')).count().sort(col('count').desc()).show()
# dm_df.groupby(col('singer')).count().sort(col('count').desc()).show()
#
# #방송미분배
# bs_df.groupby(col('album')).count().sort(col('count').desc()).show()
# bs_df.groupby(col('title')).count().sort(col('count').desc()).show()
# bs_df.groupby(col('singer')).count().sort(col('count').desc()).show()
#
# ####null 값 처리 다시하기
# album_null = ["Unknown",'앨범명 없음','unknown','앨범명없음','&nbsp;','Unknown Album','1','-','--','1-']
# title_null = []
# singer_null = ['Various Artists','Unknown','Various Artist', 'VA', 'V.A.','VARIOUS','Cover Version','Cover Artist','Unknown Artists','various', 'Varius Artist', 'VARIOUS ARTISTS', 'VARIOUS ARTISTS (UNKNOWN)']
#
#
# ###master_data, uci_data
# master_df.printSchema()
# master_df.count()
# master_df.where(col("ALBUM_NM").isNull()).count()
# master_df.where(col("CONTENT_NM").isNull()).count()
# master_df.where(col("SINGER_NM").isNull()).count()
# master_df.where(col("ALBUM_NM").isNull()).where(col("CONTENT_NM").isNull()).count()
# master_df.where(col("ALBUM_NM").isNull()).where(col("SINGER_NM").isNull()).count()
# master_df.where(col("CONTENT_NM").isNull()).where(col("SINGER_NM").isNull()).count()
#
# uci_df.printSchema()
# uci_df.count()
# uci_df.where(col("ALBUM_NM").isNull()).count()
# uci_df.where(col("CONTENT_NM").isNull()).count()
# uci_df.where(col("SINGER_NM").isNull()).count()
# uci_df.where(col("ALBUM_NM").isNull()).where(col("CONTENT_NM").isNull()).count()
# uci_df.where(col("ALBUM_NM").isNull()).where(col("SINGER_NM").isNull()).count()
# uci_df.where(col("CONTENT_NM").isNull()).where(col("SINGER_NM").isNull()).count()
# uci_df.where(col("CONTENT_NM").isNull()).where(col("SINGER_NM").isNull()).show()
#
#
# ## master ,uci data 최빈값 조회`
# master_df.groupby(col('ALBUM_NM')).count().sort(col('count').desc()).show()
# master_df.groupby(col('CONTENT_NM')).count().sort(col('count').desc()).show()
# master_df.groupby(col('SINGER_NM')).count().sort(col('count').desc()).show()
#
# uci_df.groupby(col('ALBUM_NM')).count().sort(col('count').desc()).show()
# uci_df.groupby(col('CONTENT_NM')).count().sort(col('count').desc()).show()
# uci_df.groupby(col('SINGER_NM')).count().sort(col('count').desc()).show()


###join and merge
#pf_df
total_data = pf_df.count()
pf_df = pf_df.select('*',monotonically_increasing_id())
pf_df.printSchema()

count_included_df = spark.sparkContext.emptyRDD()
schema = StructType([StructField('monotonically_increasing_id()',LongType(),True)])
count_included_df = spark.createDataFrame(count_included_df, schema)


singer_unique_master = master_df.select(col('SINGER_NM')).distinct()
new_df = pf_df.join(singer_unique_master,pf_df['singer']==singer_unique_master['SINGER_NM'], "inner")
new_df.persist()
singer_duplicate_n = new_df.count() ###singer 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()

count_included_df.distinct().count()
album_unique_master = master_df.select(col('ALBUM_NM')).distinct()
new_df = pf_df.join(album_unique_master,pf_df['album']==album_unique_master['ALBUM_NM'], "inner")
new_df.persist()
album_duplicate_n = new_df.count() ### album 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()



title_unique_master = master_df.select(col('CONTENT_NM')).distinct()
new_df = pf_df.join(title_unique_master,pf_df['title']==title_unique_master['CONTENT_NM'], "inner")
new_df.persist()
title_duplicate_n = new_df.count() ###title 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()

all_three_count = count_included_df.groupby('monotonically_increasing_id()').count().where(col('count')==3).count() ###셋다 속한 데이터
count_included_df.distinct().count() ## 셋중 하나에 속한 데이터
pf_df.count() - count_included_df.distinct().count() ## 셋중 하나에 속하지 못한 데이터


all_three_count = count_included_df.groupby('monotonically_increasing_id()').count().where(col('count')==3).count() ###셋다 속한 데이터
included_n = count_included_df.distinct().count() ## 셋중 하나에 속한 데이터
not_included_n = bs_df.count() - count_included_df.distinct().count() ## 셋중 하나에 속하지 못한 데이터
print("--------------------------pf_data--------------------------")
print(total_data ,singer_duplicate_n, album_duplicate_n, title_duplicate_n, included_n, not_included_n , sep= "\n ------------- \n ")
print( "셋다 속한 데이터:", all_three_count)

#dm_df
total_data = dm_df.count()
dm_df = dm_df.select('*',monotonically_increasing_id())
dm_df.printSchema()

count_included_df = spark.sparkContext.emptyRDD()
schema = StructType([StructField('monotonically_increasing_id()',LongType(),True)])
count_included_df = spark.createDataFrame(count_included_df, schema)


singer_unique_master = master_df.select(col('SINGER_NM')).distinct()
new_df = dm_df.join(singer_unique_master,dm_df['singer']==singer_unique_master['SINGER_NM'], "inner")
new_df.persist()
singer_duplicate_n = new_df.count() ###singer 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()

count_included_df.distinct().count()
album_unique_master = master_df.select(col('ALBUM_NM')).distinct()
new_df = dm_df.join(album_unique_master,dm_df['album']==album_unique_master['ALBUM_NM'], "inner")
new_df.persist()
album_duplicate_n =new_df.count() ### album 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()



title_unique_master = master_df.select(col('CONTENT_NM')).distinct()
new_df = dm_df.join(title_unique_master,dm_df['title']==title_unique_master['CONTENT_NM'], "inner")
new_df.persist()
title_duplicate_n = new_df.count() ###title 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()
#dm_df

all_three_count = count_included_df.groupby('monotonically_increasing_id()').count().where(col('count')==3).count() ###셋다 속한 데이터
included_n = count_included_df.distinct().count() ## 셋중 하나에 속한 데이터
not_included_n = dm_df.count() - count_included_df.distinct().count() ## 셋중 하나에 속하지 못한 데이터

print("--------------------------dm_data--------------------------")
print(total_data ,singer_duplicate_n, album_duplicate_n, title_duplicate_n, included_n, not_included_n , sep= "\n ------------- \n ")
print( "셋다 속한 데이터:", all_three_count)

#bs_df
total_data = bs_df.count()
bs_df = bs_df.select('*',monotonically_increasing_id())
bs_df.printSchema()

count_included_df = spark.sparkContext.emptyRDD()
schema = StructType([StructField('monotonically_increasing_id()',LongType(),True)])
count_included_df = spark.createDataFrame(count_included_df, schema)


singer_unique_master = master_df.select(col('SINGER_NM')).distinct()
new_df = bs_df.join(singer_unique_master,bs_df['singer']==singer_unique_master['SINGER_NM'], "inner")
new_df.persist()
singer_duplicate_n = new_df.count() ###singer 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()

count_included_df.distinct().count()
album_unique_master = master_df.select(col('ALBUM_NM')).distinct()
new_df = bs_df.join(album_unique_master,bs_df['album']==album_unique_master['ALBUM_NM'], "inner")
new_df.persist()
album_duplicate_n =new_df.count() ### album 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()



title_unique_master = master_df.select(col('CONTENT_NM')).distinct()
new_df = bs_df.join(title_unique_master,bs_df['title']==title_unique_master['CONTENT_NM'], "inner")
new_df.persist()
title_duplicate_n = new_df.count() ###title 이 겹치는 데이터의 수
count_included_df = count_included_df.union(new_df.select('monotonically_increasing_id()'))
# count_included_df.count()
new_df.unpersist()
#bs_df


all_three_count = count_included_df.groupby('monotonically_increasing_id()').count().where(col('count')==3).count() ###셋다 속한 데이터
included_n = count_included_df.distinct().count() ## 셋중 하나에 속한 데이터
not_included_n = bs_df.count() - count_included_df.distinct().count() ## 셋중 하나에 속하지 못한 데이터

print("--------------------------bs_data--------------------------")
print(total_data ,singer_duplicate_n, album_duplicate_n, title_duplicate_n, included_n, not_included_n , sep= "\n ------------- \n ")
print( "셋다 속한 데이터:", all_three_count)




# pf_df.printSchema()
# collected_df =pf_df.limit(10).collect()
# type(list())
# len(collected_df)
# collected_df[0]
# pf_df.count()
# pf_df.rdd.getNumPartitions()
# pf_df.where("album is null").count()
# pf_df.select(expr("album as al")).show()
# pf_df.selectExpr("album as al","title").show()
# from pyspark.sql import Row
# myRow = Row("hello", None, 1 ,False)
# myRow
# another_row = pf_df.first()
# another_row