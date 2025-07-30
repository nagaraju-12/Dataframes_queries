# Databricks notebook source
# MAGIC %md
# MAGIC What’s a Window Function?
# MAGIC It performs calculations across a group of rows that are related to the current row, without reducing the number of rows in the result.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
    ("U001", "BR001", "Credit", 2000, "2024-01-01"),
    ("U002", "BR002", "Debit", 1500, "2024-01-01"),
    ("U001", "BR001", "Debit", 800, "2024-01-02"),
    ("U003", "BR001", "Credit", 1200, "2024-01-03"),
    ("U002", "BR002", "Credit", 700, "2024-01-04"),
    ("U003", "BR001", "Debit", 1000, "2024-01-04"),
    ("U001", "BR001", "Debit", 500, "2024-01-05"),
    ("U002", "BR002", "Debit", 900, "2024-01-06"),
]
columns = ["user_id", "branch_id", "transaction_type", "amount", "transaction_date"]
bnk_df = spark.createDataFrame(data, columns)

# COMMAND ----------

bnk_df.show()

# COMMAND ----------

#Rank Transactions for Each Customer by Date
bnk_df=bnk_df.withColumn("rownumber",row_number().over(Window.partitionBy("user_id").orderBy("transaction_date"))).show()


# COMMAND ----------

#Running Total of Amount per Customer
from pyspark.sql.functions import sum
bnk_df=bnk_df.withColumn("total_amt",sum("amount").over(Window.partitionBy("user_id").orderBy("transaction_date"))).show()

# COMMAND ----------

#Difference Between Current and Previous Transaction
bnk_df=bnk_df.withColumn("previous_amt",lag("amount").over(Window.partitionBy("user_id").orderBy("transaction_date")))\
    .withColumn("diff",col("amount")-col("previous_amt")).show()

# COMMAND ----------

bnk_df=bnk_df.withColumn("before_amt",lead("amount").over(Window.partitionBy("user_id").orderBy("transaction_date")))\
    .withColumn("diff",col("amount")-col("before_amt")).show()

# COMMAND ----------

#Maximum Amount Till Date for Each Customer
bnk_df=bnk_df.withColumn("max_amt",max("amount").over(Window.partitionBy("user_id").orderBy("transaction_date"))).show()

# COMMAND ----------

#Rank Transactions Within Each Branch by Amount
from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window

# Define the window specification
windowSpec = Window.partitionBy("user_id").orderBy(col("amount").desc())

# Add the rank column
bnk_df = bnk_df.withColumn("rank_amt", rank().over(windowSpec))

# Show the result
bnk_df.show()


# COMMAND ----------

from pyspark.sql.functions import dense_rank, col
from pyspark.sql.window import Window

# Define the window specification
windowSpec = Window.partitionBy("branch_id").orderBy(col("amount").desc())

# Add the rank column
bnk_df = bnk_df.withColumn("dense_rank_amt", dense_rank().over(windowSpec))

# Show the result
bnk_df.show()