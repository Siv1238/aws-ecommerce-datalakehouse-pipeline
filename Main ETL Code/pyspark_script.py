import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, row_number, rank, dense_rank
from pyspark.sql.window import Window

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("RetailPipelineETL").getOrCreate()

# 2. Read Data from S3 Raw Folders
orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3://ecommerce-analytics-datalake-143/raw/orders/")
products_df = spark.read.option("header", "true").option("inferSchema", "true").csv("s3://ecommerce-analytics-datalake-143/raw/products/")

# 3. Narrow Transformation & Repartition
# Narrow: Filtering out CANCELLED orders (does not require shuffling data)
# Repartition: Optimizing partitions based on product_id before joining
clean_orders_df = orders_df.filter(col("status") != "CANCELLED").repartition(4, "product_id")

# 4. Wide Transformation & Broadcast Join
# Wide: Joining tables usually requires data shuffle.
# Broadcast: Passing the small products table to all worker nodes to avoid shuffle overhead
joined_df = clean_orders_df.join(broadcast(products_df), "product_id", "inner")

# 5. CTE (Common Table Expression) using Spark SQL
joined_df.createOrReplaceTempView("ecommerce_data")
cte_query = """
    WITH SalesCTE AS (
        SELECT 
            customer_id, 
            category, 
            product_name, 
            (price * quantity) AS total_sales
        FROM ecommerce_data
    )
    SELECT * FROM SalesCTE
"""
sales_df = spark.sql(cte_query)

# 6. Window Functions (Row Number, Rank, Dense Rank)
# Partitioning by category and ordering by total_sales descending
window_spec = Window.partitionBy("category").orderBy(col("total_sales").desc())

final_df = sales_df.withColumn("row_num", row_number().over(window_spec)) \
                   .withColumn("rank_val", rank().over(window_spec)) \
                   .withColumn("dense_rank_val", dense_rank().over(window_spec))

# 7. Coalesce & Save
# Coalesce: Reducing the number of partitions to 1 to generate a single output file
# Writing the final optimized data as a Parquet file to the processed folder
final_df.coalesce(1).write.mode("overwrite").parquet("s3://ecommerce-analytics-datalake-143/processed/")