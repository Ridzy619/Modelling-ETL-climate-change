
import configparser
import psycopg2 as ps

config = configparser.ConfigParser()
config.read("config.cfg")

properties = {}
properties['host'] = config["Redshift"]["host"]
properties['user'] = config["Redshift"]["user"]
properties['password'] = config["Redshift"]["password"]
properties['port'] = config["Redshift"]["port"]
properties['database'] = config["Redshift"]["database"]

s3_props = {}
s3_props['aws-bucket'] = config['S3']['aws-bucket']
s3_props['access-key-id'] = config['S3']['access-key-id']
s3_props['secret-access-key'] = config['S3']['secret-access-key']

def create_spark_session():
    '''Create and return a spark session instance'''
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession\
        .builder \
        .appName("Data Migration") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("fs.s3a.access.key", s3_props['access-key-id']) \
        .config("fs.s3a.secret.key", s3_props['secret-access-key']) \
        .getOrCreate()
    
    return spark
def connect_to_redshift():
    '''Return a connection instance to Redshift'''
    
    conn = ps.connect(**properties)
    
    return conn

def process(input_dir, output_dir, spark):
    '''Process the input data into facts and dimensions and write the output to the output directory.
    '''
    
    from pyspark.sql import types as T
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Define the schema
    schema = T.StructType(
        [
            T.StructField("dt", T.DateType(), True),
            T.StructField("AverageTemperature", T.DoubleType(), True),
            T.StructField("AverageTemperatureUncertainty", T.DoubleType(), True),
            T.StructField("City", T.StringType(), True),
            T.StructField("Country", T.StringType(), True),
            T.StructField("Latitude", T.StringType(), True),
            T.StructField("Longitude", T.StringType(), True)
        ]
    )
    
    # Read in demographic data and select relevant columns
    print("Reading the demographics data")
    demographic = spark.read.csv("us-cities-demographics.csv", sep=";", header=True)
    print("Done reading the demographics data")
    # Read the temperature data in
    print("Reading the temperature data")
    temperature = spark.read.csv(input_dir, header=True, schema=schema)
    print("Done reading the temperature data")
    
    # Enrich the temperature data with demographic data and filter down to United States
    temperature = demographic \
        .select(["City", "State", "Total Population"]) \
        .join(
            temperature.filter(F.col("Country")=="United States").limit(10000),
            on="City",
            how="inner"
        )
        
    
    # Filter null values out and round the numeric columns to 3 decimal places
    temperature = temperature.filter(F.col("AverageTemperature").isNotNull()) \
        .withColumn("AverageTemperature", F.round("AverageTemperature", 3)) \
        .withColumn("AverageTemperatureUncertainty", F.round("AverageTemperatureUncertainty", 3))
    
    # Date dim columns
    date_cols = [
        F.sha1(F.col('dt').cast("string")).alias('dateId'),
        F.col("dt").alias("date"),
        F.year('dt').alias('year'),
        F.month('dt').alias('month'),
        F.dayofmonth('dt').alias('day'),
        F.dayofweek('dt').alias('dayOfWeek'),
        F.weekofyear('dt').alias('weekOfYear')
    ]
    
    date_dim = temperature.select(date_cols).dropDuplicates(['dateId'])
    
    # Write the date dimension table to file
    print(f"Writing {date_dim.count()} rows of dateDim data")
    print(date_dim.printSchema())
    date_dim.write.parquet(output_dir + "/dateDim", mode='overwrite')

    # Location Dim columns
    loc_cols = [
        F.sha1(F.concat("City", "Country")).alias("locationId"),
        F.col('City').alias('city'),
        F.col('State').alias('state'),
        F.col('Country').alias('country'),
        F.col('Latitude').alias('latitude'),
        F.col('Longitude').alias('longitude'),
        F.col('Total Population').alias('totalPopulation')
    ]
    
    location_dim = temperature.select(loc_cols).drop_duplicates(subset=["locationId"])
    
    # Write locations dimension to file
    print(f"Writing {location_dim.count()} rows of locationDim data")
    print(location_dim.printSchema())
    location_dim.write.parquet(output_dir + "/locationDim", mode='overwrite')
    
    # temperature Fact columns
    temp_cols = [
        F.row_number().over(Window.partitionBy().orderBy("Country")).alias("temperatureId"),
        F.sha1(F.col('dt').cast("string")).alias('dateId'),
        F.sha1(F.concat("City", "Country")).alias("locationId"),
        F.col("AverageTemperature").alias('avgTemp'),
        F.col("AverageTemperatureUncertainty").alias('avgTempUncert')
    ]
    
    temp_fact = temperature.select(temp_cols)
    
    # Write temperature facts table to file
    print(f"Writing {temp_fact.count()} rows of temperatureFact data")
    print(temp_fact.printSchema())
    temp_fact.write.parquet(output_dir + "/temperatureFact", mode='overwrite')
    
    

def copy_to_db(cur, conn):
    from sql_queries import copy_tables
    
    for copy_sql in copy_tables:
        copy_sql = copy_sql.format(
            aws_bucket=s3_props['aws-bucket'],
            aws_key="output2",
            access_key_id=s3_props['access-key-id'],
            secret_access_key=s3_props['secret-access-key']
        )
        print(copy_sql)
        cur.execute(copy_sql)
        conn.commit()
        
def create_tables(cur, conn):
    '''Create all necessary tables'''
    from sql_queries import create_tables
    
    for create_sql in create_tables:
        cur.execute(create_sql)
        conn.commit()

def drop_tables(cur, conn):
    from sql_queries import drop_tables
    
    for drop_sql in drop_tables:
        cur.execute(drop_sql)
        conn.commit()
        
def check_data_exists(cur):
    from sql_queries import data_exists
    tables = ['dateDim', 'locationDim', 'temperatureFact']
    
    # Check that data exists
    for table in tables:
        cur.execute(data_exists.format(table=table))
        result = cur.fetchone()
        
        assert len(result)>0, f"Data quality Failed on table {table}"
        assert result[0]>0, f"Data quality Failed on table {table}"
        
    print("Data quality checks passed")

def check_null(cur):
    from sql_queries import null_constraint_check
    checks = {
        "locationDim":["city", "state", "country", "latitude", "longitude"],
        "dateDim":["date", "year", "month", "day", "dayofweek", "weekofyear"],
        "temperatureFact":["avgTemp", "avgTempUncert"]
    }
    
    for table, columns in checks.items():
        for column in columns:
            cur.execute(null_constraint_check.format(column=column, table=table))
            result = cur.fetchone()
            assert result is None, f"Column {column} in table {table} does not satisfy null constraint!"
    
    print("All null constraints passed")
        
def main():
    print("Connecting to spark")
    spark = create_spark_session()
    print("Done!")
    
    print("Connecting to Redshift")
    conn = connect_to_redshift()
    print("Done!")
    cur = conn.cursor()
    
    input_dir = 's3a://data-mig-project/input/GlobalLandTemperaturesByCity.csv'
    output_dir = "s3a://data-mig-project/output2"
    # output_dir = "output"
    
    # Process and write files to output_dir
    print("Processing file with spark")
    process(input_dir, output_dir, spark)
    print("Done!")
    
    # Drop existing tables
    print("Dropping existing tables")
    drop_tables(cur, conn)
    print("Done!")
    
    # Create new tables
    print("Creating new tables")
    create_tables(cur, conn)
    print("Done!")
    
    # Copy to files to DB
    print("Copying files to Redshift")
    copy_to_db(cur, conn)
    print("Done!")
    
    
    # Check data quality
    check_data_exists(cur)
    
    # Check for null constraints
    check_null(cur)
    
if __name__=="__main__":
    main()