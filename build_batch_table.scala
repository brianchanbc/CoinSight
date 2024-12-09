import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
// spark-shell --jars ./iw-1.0-SNAPSHOT.jar
// Create Spark Session
val spark = SparkSession.builder.appName("Build crypto tables").master("local[*]").enableHiveSupport().getOrCreate()

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS brianchan_all_crypto_prices (
    pair STRING,
    time TIMESTAMP,
    open DOUBLE,
    close DOUBLE,
    high DOUBLE,
    low DOUBLE,
    volume DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/brianchan/datalake/combined_data'
TBLPROPERTIES ("skip.header.line.count"="1")
""")

// Query the table and create a new DataFrame with readable date
val all_crypto_prices_with_readable_date = spark.sql("""
SELECT pair, to_date(from_unixtime(time / 1000)) as date, from_unixtime(time / 1000) as time, open, close, high, low, volume
FROM brianchan_all_crypto_prices
WHERE pair != 'pair'
""")

// Show the result
all_crypto_prices_with_readable_date.show()

all_crypto_prices_with_readable_date.write.mode(SaveMode.Overwrite).saveAsTable("brianchan_all_crypto_prices")

// screen -S spark_job
// # To detach: Press Ctrl+A then D
// screen -ls
// # While in screen, press: Ctrl + a, then [ to move up and down
// screen -r spark_job
