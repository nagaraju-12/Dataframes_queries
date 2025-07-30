# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("BankTxnGroupBy").getOrCreate()
Data=[("U001", "BR001", "Credit", 2000, "2024-01-01"),
    ("U002", "BR002", "Debit", 1500, "2024-01-01"),
    ("U001", "BR001", "Debit", 800, "2024-01-02"),
    ("U003", "BR001", "Credit", 1200, "2024-01-03"),
    ("U002", "BR002", "Credit", 700, "2024-01-04"),
    ("U003", "BR001", "Debit", 1000, "2024-01-04"),
    ("U001", "BR001", "Debit", 500, "2024-01-05"),
    ("U002", "BR002", "Debit", 900, "2024-01-06")]
bank_schema=StructType([StructField("user_id",StringType(),True),
                        StructField("branch_id",StringType(),True),
                        StructField("txn_type",StringType(),True),
                        StructField("txn_amt",IntegerType(),True),
                        StructField("txn_date",StringType(),True)
                        ])
bank_df=spark.createDataFrame(Data,bank_schema)
bank_df.display()

# COMMAND ----------

bank_df.printSchema()

# COMMAND ----------

bank_df=bank_df.withColumn("txn_date",bank_df["txn_date"].cast("date"))
bank_df.printSchema()

# COMMAND ----------

bank_df.show()

# COMMAND ----------

#Total transaction amount per branch
from pyspark.sql.functions import sum
bank_df=bank_df.groupBy("Branch_id").agg(sum("txn_amt")).show()

# COMMAND ----------

from pyspark.sql.functions import col
bank_df = bank_df.withColumn("txn_amt", col("txn_amt").cast("double"))


# COMMAND ----------

#Total credit & debit amount per customer
from pyspark.sql.functions import sum
bank_df=bank_df.groupBy("user_id","txn_type").agg(sum("txn_amt").alias("total_amt"))
bank_df.show()

# COMMAND ----------

#Count of transactions per day
from pyspark.sql.functions import count
bank_df=bank_df.groupBy("txn_date").agg(count("txn_amt").alias("txn_count"))
bank_df.show()

# COMMAND ----------

 #Min & Max transaction amount per customer
 from pyspark.sql.functions import min, max
 bank_df=bank_df.groupBy("user_id").agg(min("txn_amt").alias("min_amt"),max("txn_amt").alias("max_amt"))
 bank_df.show()

# COMMAND ----------

# Average transaction amount per branch
from pyspark.sql.functions import avg
bank_df=bank_df.groupBy("branch_id").agg(avg("txn_amt").alias("avg_amt")).show()

# COMMAND ----------

#“For each branch, show the total debit and credit amounts separately”
from pyspark.sql.functions import sum
bank_df=bank_df.groupBy("branch_id","txn_type").agg(sum("txn_amt").alias("total_amt")).show()