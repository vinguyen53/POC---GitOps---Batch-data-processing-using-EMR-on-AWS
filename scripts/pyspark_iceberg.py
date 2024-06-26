from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
import argparse

#get argument
parser = argparse.ArgumentParser()
parser.add_argument("--input_url")
args = parser.parse_args()

#config iceberg
conf = (SparkConf()
        .set("spark.sql.catalog.demo","org.apache.iceberg.spark.SparkCatalog",)
        .set("spark.sql.catalog.demo.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
        .set("spark.sql.catalog.demo.warehouse","s3://ntanvi-sfn-emr-demo/data_target/iceberg_warehouse")
        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # .set("spark.executor.memory","2G")
        # .set("spark.executor.cores","2")
        )

# Create SparkSession
spark = (SparkSession
         .builder
         .config('spark.jars',"/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar")
         .config(conf=conf)
         .getOrCreate())

# Specify the input file path
input_file = args.input_url

# Read CSV into a DataFrame
df = spark.read.option("header", "true").csv(input_file)
df = df.withColumn("id", df["id"].cast(IntegerType()))

#create iceberg database
spark.sql("CREATE DATABASE IF NOT EXISTS demo.iceberg_db")

#crate iceberg table
spark.sql("""CREATE TABLE IF NOT EXISTS demo.iceberg_db.iceberg_table 
          (
            id int,
            marketplace string,
            customer_id string,
            product_id string,
            seller_id string,
            sell_date string,
            quantity string
          )
            USING iceberg
            location 's3://ntanvi-sfn-emr-demo/data_target/iceberg_warehouse/iceberg_table'
          """)

#create new data temp table
df.createOrReplaceTempView("new_data")

#upsert into iceberg table
spark.sql('''
    MERGE INTO demo.iceberg_db.iceberg_table as t
    USING new_data as s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
''')

#write data into iceberg_table
# df.write.format("iceberg").mode("append").save("demo.iceberg_db.iceberg_table ")

#read data from iceberg_table
# df = spark.read.format('iceberg').table('demo.iceberg_db.iceberg_table')

# df.show()

# Stop the SparkSession
spark.stop()
