from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from datetime import datetime
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

import sys
spark = SparkSession.builder.appName("compute").getOrCreate()

code_home = os.getcwd()

config_file_path = os.path.join(code_home, "config.json")
f = open(config_file_path)
config = json.load(f)
 
accessKey = config["S3AccessKey"]
secretKey=config["S3SecretKey"]
yr_month=sys.argv[1]
#yr_month=200011
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKey)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretKey)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "ap-southeast-1.amazonaws.com")

#df_csv=spark.read.csv('C:\\Users\\dz113sf\\Downloads\\ratings_Movies_and_TV.csv')
df_csv=spark.read.json("s3a://ratings_Movies_and_TV.csv")
new_df_csv=df_csv.withColumnRenamed('_c0','reviewer_id').withColumnRenamed('_c1','product_id').withColumnRenamed('_c2','rating').withColumnRenamed('_c3','unixtime_value')
movies_rating_df=new_df_csv.withColumn('rating_month',from_unixtime(col('unixtime_value'),"yyyy-MM-dd HH:mm:ss"))

df_json=spark.read.json("s3a://meta_Movies_and_TV.json")
#df_json=spark.read.json('C:\\Users\\dz113sf\\Downloads\\meta_Movies_and_TV.json').filter(col('asin').isNotNull())

def avg_rating_by_month(yr_month,df):
    avg_rating_df=df.filter(col('yr_month') == yr_month)
    return avg_rating_df

def allotRanks(df, col='avg_rating'):
    rank_wind=Window.orderBy(col)
    rank_df=df.withColumn('rank',row_number().over(rank_wind))
    return rank_df

def calculate_prev_mth(yr_month):
    curr_yr = yr_month[:4]
    curr_mth = yr_month[4:]
    if curr_mth == "1":
        prev_yr = str(int(curr_yr)-1)
        prev_mth = "12"
    else:
        prev_yr = curr_yr
        prev_mth = str(int(curr_mth)-1)
       
    return (prev_yr+prev_mth)

def bottom_n_movies(df,n):
    return (df.filter(col('rank') <=n))

def top_n_movies(df,n):
    max_rank=df.agg(max(col('rank')).alias('max_rank')).select('max_rank').collect()[0].asDict().values()
    from_rank=list(max_rank)[0] -n+1
    print('max_rank ',max_rank)
	print('from_rank ',from_rank)
    top_5_movies=df.filter(col('rank') >=max_rank)
    #top_5_movies.show()
    return top_5_movies

metajson_df=df_json.withColumn(
    'explode_value',explode(col('categories'))
).filter((col('explode_value')[0] == 'Movies & TV') & (col('explode_value')[1] == 'Movies')
        )

#movies_rating_df.show()
#metajson_df.show()
joined_df=movies_rating_df.alias('t1').join(
    metajson_df.alias('t2'),col('t1.product_id') == col('t2.asin'),'inner'
    ).withColumn('yr_month',concat(year('rating_month'),month('rating_month'))
                ).select(
    't1.product_id','t1.rating','yr_month'
)

avg_rating_df=joined_df.groupBy('yr_month','product_id').agg(avg('rating').alias('avg_rating'))


rank_wind=Window.partitionBy('yr_month').orderBy('avg_rating')
#rank_df=avg_rating_df.withColumn('rank',row_number().over(rank_wind))
#rank_df.orderBy('yr_month').filter((col('yr_month') == '199711') | (col('yr_month') == '199710')).show(1000)

#bottom_5_calc
monthly_avg_df = avg_rating_by_month(yr_month,avg_rating_df)
rank_df = allotRanks(monthly_avg_df)
uc1_bottom_five_mv = bottom_n_movies(rank_df, 5)
uc1_bottom_five_mv.show()

#top 5 movies
uc1_top_five_mv = top_n_movies(rank_df, 5)
uc1_top_five_mv.show()

#Case Two: For a given month, what are the 5 movies whose average monthly ratings increased the most compared with the previous month?
yr_month='200011'
curr_avg_df = avg_rating_by_month(yr_month,avg_rating_df)
prev_yr_mth = calculate_prev_mth(yr_month)
print('prev_yr_mth ',prev_yr_mth)
prev_avg_df = avg_rating_by_month(prev_yr_mth,avg_rating_df)
prev_avg_df.show(100)
print('current below........')
curr_avg_df.show(100)
diff_joined_df = prev_avg_df.alias(
        'prev').join(
    curr_avg_df.alias('curr'),'product_id','inner'
    ).select(
        'product_id',
        col('curr.yr_month').alias('curr_yr_month'),
         col('prev.yr_month').alias('prev_yr_month'),
         col('curr.avg_rating').alias('curr_avg_rating'),
          col('prev.avg_rating').alias('prev_avg_rating')
        ).withColumn(
            'diff_avg_rating',lit(col('curr_avg_rating') - col('prev_avg_rating'))
    )
diff_joined_ranked_df = allotRanks(diff_joined_df, 'diff_avg_rating')
diff_joined_ranked_df.show()
top_five_mv = top_n_movies(diff_joined_ranked_df, 5)
top_five_mv.show()


