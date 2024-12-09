import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create Spark Session
val spark = SparkSession.builder.appName("Combine Crypto CSVs").master("local[*]").getOrCreate()

// Read all CSV files from HDFS directory
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/brianchan/downloads/*.csv")

// Add filename column and extract pair name
val dfWithPair = df.withColumn("filename", input_file_name()).withColumn("pair", substring_index(substring_index(col("filename"), "/", -1), ".", 1))

// Select only the columns we want in the desired order
val finalDf = dfWithPair.select(
  col("pair"),
  col("time"),
  col("open"),
  col("close"),
  col("high"),
  col("low"),
  col("volume")
)

// Write to a single CSV file
finalDf.coalesce(1).write.mode("overwrite").option("header", "true").csv("/brianchan/datalake/combined_data")

// hdfs dfs -ls /brianchan/downloads/
// hdfs dfs -ls /brianchan/datalake/combined_data
// hdfs dfs -cat /brianchan/datalake/combined_data/combined_data.csv | head -n 10
// hdfs dfs -cat /brianchan/datalake/combined_data.csv | wc -l

// 136980167 rows
