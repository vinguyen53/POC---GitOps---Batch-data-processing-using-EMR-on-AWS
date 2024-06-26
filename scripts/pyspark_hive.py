from pyspark.sql import SparkSession
from pyspark import SparkConf

#config hive
conf = (SparkConf()
        .set("spark.hadoop.hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.sql.warehouse.dir","s3://ntanvi-sfn-emr-demo/data_target/hive_warehouse")
        )

# Create SparkSession
spark = (SparkSession
         .builder
         .config(conf=conf)
         .enableHiveSupport()
         .getOrCreate())

# Specify the input file path
input_file = 's3://ntanvi-sfn-emr-demo/data_source/product_data.csv'

# Read CSV into a DataFrame
df = spark.read.option("header", "true").csv(input_file)


print(df.show())

df.printSchema()

#create hive database
spark.sql("CREATE DATABASE IF NOT EXISTS hive_db")

#crate hive table
spark.sql("""CREATE TABLE IF NOT EXISTS hive_db.hive_table 
          (
            id int,
            marketplace string,
            customer_id string,
            product_id string,
            seller_id string,
            sell_date string,
            quantity string
          )
          USING hive
          OPTIONS(fileFormat 'parquet')
          """)


# Write DataFrame as Parquet to the output folder
df.write.mode("overwrite").format('hive').saveAsTable("hive_db.hive_table")

hive_df = spark.sql('select * from hive_db.hive_table')
print('hive df')
hive_df.show()

# Stop the SparkSession
spark.stop()
