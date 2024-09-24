from pyspark.sql import SparkSession
from cassandra.util import datetime_from_uuid1
from cassandra.cqltypes import TimeUUIDType
import uuid 
import time
from uuid import UUID
import time_uuid 
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as sf
import datetime



spark = SparkSession.builder \
    .appName("Hien's project") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.32,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
# 'localhost' if Spark is outside Docker, 'cassandra' if Spark is inside Docker

def filter_data(df,mysql_latest_time):
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df = df.filter(df.ts.isNotNull())
    df = df.withColumn("ts", sf.split(col("ts"), "\\.").getItem(0))
    # print("After Split:")
    # df.show(truncate=False)
    df = df.withColumn("ts", sf.to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("bid", col("bid").cast("double"))
    df = df.where(col('ts')> mysql_latest_time)
    return df


def process_click_data(df):
    click_data = df.filter(df.custom_track == 'click')
    click_data.createOrReplaceTempView('click_data')
    return spark.sql("""select date(ts) as date,
                        hour(ts) as hour ,
                        job_id,
                        publisher_id,
                        campaign_id,
                        group_id,
                        round(avg(bid),2) as bid_set , 
                        sum(bid) as spend_hour , 
                        count(*) as click 
                        from click_data 
                        group by date(ts), hour(ts), job_id, publisher_id, campaign_id, group_id""")
    
    
def process_conversion_data(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data.createOrReplaceTempView('conversion_data')
    return spark.sql("""select date(ts) as date,
                                hour(ts) as hour ,
                                job_id,
                                publisher_id,
                                campaign_id,
                                group_id, 
                                count(*) as conversion 
                                from conversion_data 
                                group by date(ts), hour(ts),job_id,publisher_id,campaign_id,group_id""")
    

def process_qualified_data(df):
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data.createOrReplaceTempView('qualified_data')

    return spark.sql("""select date(ts) as date,
                               hour(ts) as hour,
                               job_id,
                               publisher_id,
                               campaign_id,
                               group_id, 
                               count(*) as qualified 
                               from qualified_data 
                               group by date(ts),hour(ts),job_id,publisher_id,campaign_id,group_id""")
    
    
def process_unqualified_data(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data.createOrReplaceTempView('unqualified_data')
    return spark.sql("""select date(ts) as date,
                                 hour(ts) as hour ,
                                 job_id,
                                 publisher_id,
                                 campaign_id,
                                 group_id, 
                                 count(*) as unqualified 
                                 from unqualified_data 
                                 group by date(ts), hour(ts),job_id,publisher_id,campaign_id,group_id""")
    
    
def process_cassandra_data(df):
    click_data = process_click_data(df)
    conversion_data = process_conversion_data(df)
    qualified_data = process_qualified_data(df)
    unqualified_data = process_unqualified_data(df)
    
    assert click_data is not None, "click_data is None"
    assert conversion_data is not None, "conversion_data is None"
    assert qualified_data is not None, "qualified_data is None"
    assert unqualified_data is not None, "unqualified_data is None"

    final = click_data.join(conversion_data, on=['date', 'hour', 'job_id', 'publisher_id', 'campaign_id', 'group_id'], how='full') \
        .join(qualified_data, on=['date', 'hour', 'job_id', 'publisher_id', 'campaign_id', 'group_id'], how='full') \
        .join(unqualified_data, on=['date', 'hour', 'job_id', 'publisher_id', 'campaign_id', 'group_id'], how='full')
    return final

def finalize_data(df,cassandra_latest_time):
    df = df.withColumn('updated_at',sf.lit(cassandra_latest_time))
    df = df.withColumn('sources',sf.lit('Cassandra'))
    df = df.withColumnRenamed('date','dates')
    df = df.withColumnRenamed('hour','hours')
    df = df.withColumnRenamed('qualified','qualified_application')
    df = df.withColumnRenamed('unqualified','disqualified_application')
    df = df.withColumnRenamed('click','clicks')
    
    df = df.na.fill({'disqualified_application':0})
    df = df.na.fill({'qualified_application':0})
    df = df.na.fill({'clicks':0})
    df = df.na.fill({'conversion':0})
    
    return df.select(
    'job_id',
    'dates',
    'hours',
    'disqualified_application',
    'qualified_application',
    'conversion',
    'company_id',
    'group_id',
    'campaign_id',
    'publisher_id',
    'bid_set',
    'clicks',
    'spend_hour',
    'sources',
    'updated_at')
    
    
def get_data_from_mysql(host = 'localhost' ,port = '3306', db_name = 'mydatabase',
                        driver = "com.mysql.cj.jdbc.Driver", user = 'myuser', password = 'mypassword',
                        sql_query = '(SELECT id as job_id, company_id FROM job) A'):
    
    return spark.read.format('jdbc').options(url = f'jdbc:mysql://{host}:{port}/{db_name}' , 
                                             driver = driver , 
                                             dbtable = sql_query , 
                                             user=user , 
                                             password = password).load()
    

def write_data_to_mysql(df,host = 'localhost',port = '3306',
                        db_name = 'mydatabase', user = 'myuser', table = 'events',
                        password = 'mypassword', driver = "com.mysql.cj.jdbc.Driver"):
    df.write.format("jdbc") \
    .option("driver",driver) \
    .option("url", f"jdbc:mysql://{host}:{port}/{db_name}") \
    .option("dbtable", table) \
    .mode("append") \
    .option("user", user) \
    .option("password", password) \
    .save()
    
def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'de_project').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(host,port,db_name,driver,user,password):    
    sql_query = """(select max(updated_at) from events) data"""
    mysql_time = get_data_from_mysql(host,port,db_name,driver,user,password,sql_query)
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest_time = '1970-01-01 23:59:59'
    else:
        mysql_latest_time = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest_time 

    # print("Get data from Cassandra")
    
    # # Convert mysql_latest_time to a format compatible with Cassandra's ts column
    # # Assuming mysql_latest_time is a datetime object
    # mysql_latest_time_str = mysql_latest_time.strftime("%Y-%m-%d %H:%M:%S")
    
    # # Read from Cassandra with a WHERE clause
    # df = spark.read \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .options(table='tracking', keyspace='de_project') \
    #     .option("pushdown", "true") \
    #     .option("filter", f"ts > '{mysql_latest_time_str}'") \
    #     .load()
    
    # # If you still need to apply the filter in Spark (for any reason), you can do:
    # # df = df.filter(col('ts') > mysql_latest_time)
    
    # print(f"Loaded {df.count()} rows from Cassandra")
def main_task(cassandra_latest_time,mysql_latest_time):

    print("Get data from cassandra")
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'de_project').load()

    print('Filter out the data')
    df = filter_data(df,mysql_latest_time)
   
    
    print('Process the data')
    df = df.cache()
    df = process_cassandra_data(df) 
    
    print('get company data from mysql')
    jobs = get_data_from_mysql(host = host,port = port,db_name = db_name,user = user,password = password, driver = driver,
                               sql_query = '(SELECT id as job_id, company_id FROM job) A')
    
    print('Merge the data with company data')
    df = df.join(jobs,on = 'job_id',how='left')
    
    print('finalizing the data')
    df = finalize_data(df,cassandra_latest_time)
    print('final ouput')
    print(df.show(truncate=False))
    
    print('Write the data to mysql')
    write_data_to_mysql(df = df,host = host,port = port,db_name = db_name,user = user,password = password,driver = driver)
    
    print('done')
    
    
#MySQL Credentials
host = 'mysql'
port = '3306'
db_name = 'mydatabase'
user = 'myuser'
password = 'mypassword'
driver = "com.mysql.cj.jdbc.Driver"


while True:
    start_time = datetime.datetime.now()
    cassandra_latest_time = get_latest_time_cassandra()
    mysql_latest_time = get_mysql_latest_time(host,port,db_name,driver,user,password)

    mysql_latest_time = datetime.datetime.strptime(mysql_latest_time, '%Y-%m-%d %H:%M:%S')
    if '.' in cassandra_latest_time:
        cassandra_latest_time = cassandra_latest_time.split('.')[0]
    cassandra_latest_time = datetime.datetime.strptime(cassandra_latest_time, '%Y-%m-%d %H:%M:%S')
           
    print(f'cassandra_latest_time :  {cassandra_latest_time}')
    print(f'mysql_latest_time :  {mysql_latest_time}')
    
    if cassandra_latest_time > mysql_latest_time:
        print(f"bruh cassandra_latest_time: {cassandra_latest_time} > mysql_latest_time: {mysql_latest_time}")
        main_task(cassandra_latest_time,mysql_latest_time)
    else:
        print("No new data found in cassandra")
    
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(30)
    
    
