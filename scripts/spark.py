import os
import sys
import shutil

# ==========================================
# 1. Environment Configurations
# ==========================================
os.environ["JAVA_HOME"] = r"C:\Java\jdk-17"
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python"))
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.10.9.7-src.zip")) 

base_output_dir = "analytics_results"
for folder in [base_output_dir, "cleaned_master_data"]:
    if os.path.exists(folder):
        try:
            shutil.rmtree(folder)
            print(f"Cleaned up old directory: {folder}")
        except Exception as e:
            print(f"Warning: Could not delete {folder} automatically: {e}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, trim, to_date, year, month, dayofmonth, quarter, size, lower, avg, when, count, explode, sha2, date_format

# ==========================================
# 2. Spark Session Initialization
# ==========================================
spark = SparkSession.builder \
    .appName("ArXivMedallionPipeline") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("Spark Session Initialized successfully.\n")

db_url = "jdbc:postgresql://localhost:5432/arxiv_db"
db_properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

try:
    # ==========================================
    # 3. Ingestion & Pre-processing (Bronze to Silver)
    # ==========================================
    csv_file_path = "large_raw_arxiv_papers.csv"
    raw_df = spark.read.option("header", "true") \
                       .option("multiLine", "true") \
                       .option("escape", '"') \
                       .csv(csv_file_path)

    cleaned_df = raw_df \
        .filter(col("Title").isNotNull() & col("Abstract").isNotNull() & col("Published_Date").isNotNull()) \
        .withColumn("Title_Cleaned", trim(regexp_replace(col("Title"), r"[\n\r\t]+", " "))) \
        .withColumn("Abstract_Cleaned", trim(regexp_replace(col("Abstract"), r"[\n\r\t]+", " "))) \
        .withColumn("Publish_Date_Parsed", to_date(col("Published_Date"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("Publish_Year", year(col("Publish_Date_Parsed"))) \
        .withColumn("Authors_Array", split(col("Authors"), r",\s*")) \
        .withColumn("Author_Count", size(col("Authors_Array"))) \
        .withColumn("Main_Category", col("Category")) \
        .filter(col("Publish_Year").isNotNull())

    cleaned_df = cleaned_df \
        .withColumn("Title_Word_Count", size(split(col("Title_Cleaned"), r"\s+"))) \
        .withColumn("Abstract_Word_Count", size(split(col("Abstract_Cleaned"), r"\s+")))

    # ==========================================
    # 4. Dimensional Modeling (Database Loads)
    # ==========================================
    
    # Dimension 1: Dim_Categories
    print("Loading Dim_Categories to PostgreSQL...")
    dim_categories_df = cleaned_df.select("Main_Category").distinct() \
        .withColumnRenamed("Main_Category", "category_name") \
        .filter(col("category_name") != "")
    
    try:
        existing_categories = spark.read.jdbc(url=db_url, table="dim_categories", properties=db_properties).select("category_name")
        dim_categories_write = dim_categories_df.join(existing_categories, on="category_name", how="left_anti")
    except Exception:
        dim_categories_write = dim_categories_df

    dim_categories_write.write.jdbc(url=db_url, table="dim_categories", mode="append", properties=db_properties)

    # Dimension 2: Dim_Authors
    print("Loading Dim_Authors to PostgreSQL...")
    authors_exploded = cleaned_df.select(explode(col("Authors_Array")).alias("author_name"))
    dim_authors_df = authors_exploded.withColumn("author_name", trim(col("author_name"))).distinct() \
        .filter((col("author_name") != "") & (col("author_name").isNotNull()))
        
    try:
        existing_authors = spark.read.jdbc(url=db_url, table="dim_authors", properties=db_properties).select("author_name")
        dim_authors_write = dim_authors_df.join(existing_authors, on="author_name", how="left_anti")
    except Exception:
        dim_authors_write = dim_authors_df

    dim_authors_write.write.jdbc(url=db_url, table="dim_authors", mode="append", properties=db_properties)

    # Dimension 3: Dim_Time
    print("Loading Dim_Time to PostgreSQL...")
    dim_time_df = cleaned_df.select("Publish_Date_Parsed").distinct() \
        .withColumn("date_id", date_format(col("Publish_Date_Parsed"), "yyyyMMdd").cast("int")) \
        .withColumn("full_date", col("Publish_Date_Parsed")) \
        .withColumn("day", dayofmonth(col("Publish_Date_Parsed"))) \
        .withColumn("month", month(col("Publish_Date_Parsed"))) \
        .withColumn("year", year(col("Publish_Date_Parsed"))) \
        .withColumn("quarter", quarter(col("Publish_Date_Parsed"))) \
        .select("date_id", "full_date", "day", "month", "year", "quarter")
        
    try:
        existing_time = spark.read.jdbc(url=db_url, table="dim_time", properties=db_properties).select("date_id")
        dim_time_write = dim_time_df.join(existing_time, on="date_id", how="left_anti")
    except Exception:
        dim_time_write = dim_time_df

    dim_time_write.write.jdbc(url=db_url, table="dim_time", mode="append", properties=db_properties)

    # ==========================================
    # 5. Fact Table Mapping & Load
    # ==========================================
    print("Building and loading Fact_Papers to PostgreSQL...")
    
    db_categories = spark.read.jdbc(url=db_url, table="dim_categories", properties=db_properties)
    
    fact_df = cleaned_df \
        .join(db_categories, cleaned_df.Main_Category == db_categories.category_name, "left") \
        .withColumn("publish_date_id", date_format(col("Publish_Date_Parsed"), "yyyyMMdd").cast("int")) \
        .withColumn("paper_id", sha2(col("Title_Cleaned"), 256))
        
    fact_final_df = fact_df.select(
        col("paper_id"),
        col("Title_Cleaned").alias("title"),
        col("Abstract_Cleaned").alias("abstract"),
        col("publish_date_id"),
        col("category_id"),
        col("Title_Word_Count").alias("title_word_count"),
        col("Abstract_Word_Count").alias("abstract_word_count"),
        col("Author_Count").alias("author_count")
    ).distinct()

    fact_final_df.write.jdbc(url=db_url, table="fact_papers", mode="append", properties=db_properties)

    # ==========================================
    # 6. Local Analytics Exports (Gold Layer Backup)
    # ==========================================
    print("Saving analytical reports locally...")
    
    collab_df = cleaned_df.groupBy("Publish_Year").agg(
        avg("Author_Count").alias("Avg_Authors_Per_Paper"),
        count(when(col("Author_Count") == 1, 1)).alias("Solo_Papers_Count"),
        count("Title_Cleaned").alias("Total_Papers")
    ).withColumn("Solo_Papers_Percentage", (col("Solo_Papers_Count") / col("Total_Papers")) * 100).orderBy("Publish_Year")
    
    collab_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(base_output_dir, "collaboration_index"))

    print("Archiving Master Cleaned Data in Parquet format...")
    cleaned_df.write.mode("overwrite").partitionBy("Publish_Year", "Main_Category").parquet("cleaned_master_data")

    print("\nDatabase synchronization complete.")

except Exception as e:
    print(f"Error encountered during pipeline execution: {e}")

finally:
    spark.stop()
    print("Spark Session Closed.")