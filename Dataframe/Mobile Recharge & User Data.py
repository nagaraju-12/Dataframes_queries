# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Spark DataFrames").getOrCreate()


# COMMAND ----------


from pyspark.sql.types import *
from datetime import datetime

user_data=[("U001", "Raj", "Mumbai", "2024-01-01"),
    ("U002", "Sneha", "Delhi", "2024-01-05"),
    ("U003", "Akash", "Chennai", "2024-01-10"),
    ("U004", "Priya", "Bangalore", "2024-01-12"),
    ("U005", "Arjun", "Hyderabad", "2024-01-15"),
    ("U006", "Anjali", "Delhi", "2024-01-18"),
    ("U007", "Kiran", "Mumbai", "2024-01-20"),
    ("U008", "Meena", "Chennai", "2024-01-22")]
user_schema=StructType([
    StructField("user_id",StringType(),True),
    StructField("name",StringType(),True),
    StructField("city",StringType(),True),StructField("activation_date",StringType(),True)
])
data=spark.createDataFrame(user_data,user_schema)
data.printSchema()
data.show()

# COMMAND ----------

data.printSchema()
data.show()

# COMMAND ----------

#here change the datatype of activation_date from string to date
from datetime import datetime
data=data.withColumn("activation_date",data["activation_date"].cast("date"))
data.printSchema()
data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample recharge data

# COMMAND ----------

from pyspark.sql.types import *
from datetime import datetime

recharge_data =[("U001", "2024-01-03", "Data", 249),
    ("U001", "2024-01-10", "Talktime", 100),
    ("U002", "2024-01-15", "Combo", 349),
    ("U003", "2024-01-11", "Data", 199),
    ("U003", "2024-01-15", "SMS", 50),
    ("U004", "2024-01-20", "Combo", 399),
    ("U005", "2024-01-25", "Data", 299),
    ("U005", "2024-01-30", "Talktime", 100),
    ("U006", "2024-02-01", "Combo", 449),
    ("U007", "2024-02-02", "Data", 149),
    ("U008", "2024-02-05", "SMS", 49),
    ("U008", "2024-02-10", "Data", 199),
    ("U002", "2024-01-30", "Talktime", 100),
    ("U004", "2024-01-28", "Data", 249),
    ("U007", "2024-02-08", "Combo", 399),
    ("U006", "2024-02-10", "Talktime", 100)]
recharge_schema=StructType([StructField("user_id",StringType(),True),
                           StructField("recharge_date",StringType(),True),
                           StructField("recharge_type",StringType(),True),
                           StructField("amount",IntegerType(),True)])

data1=spark.createDataFrame(recharge_data,recharge_schema)
data1.printSchema()
data1.show()

# COMMAND ----------

data1.printSchema()

# COMMAND ----------

data1=data1.withColumn("recharge_date",data1["recharge_date"].cast("date"))
data1.printSchema()
data1.show()


# COMMAND ----------

#Total Recharge per User
from pyspark.sql.functions import sum
data1=data1.groupBy("user_id").agg(sum("amount").alias("total_recharges"))
data1.show()
                                   

# COMMAND ----------

data1.printSchema()


# COMMAND ----------

# First Recharge Date for Each User
from pyspark.sql.functions import row_number,col
from pyspark.sql.window import Window
data1=data1.withColumn("row_number",row_number().over(Window.partitionBy("user_id").orderBy(col("recharge_date").asc())))
first_recharge_df=data1.filter(col("row_number")==1).select("user_id", "row_number","recharge_date", "recharge_type", "amount").show()


# COMMAND ----------

# Join: Add City and Name to Recharge Data
data2=data.join(data1, data["user_id"]==data1["user_id"], "inner")
data2.show()

# COMMAND ----------

# Join: Add City and Name to Recharge Data
join_data=data1.join(data, on="user_id", how="inner")
join_data.select("user_id","city","name","recharge_date","recharge_type","amount").show()

# COMMAND ----------

join_data=join_data.filter(col("recharge_type")=='Combo')
join_data.show()

# COMMAND ----------

#Total Recharge by City
joined_data=join_data.groupBy("city").agg(sum("amount").alias("total_recharges"))
joined_data.show()

# COMMAND ----------

# Filter: Users who did only ‘Combo’ recharges
from pyspark.sql.functions import collect_set, size, array_contains, col

# Step 1: Group and collect recharge types
combo_data = join_data.groupBy("user_id") \
    .agg(collect_set("recharge_type").alias("recharge_type1"))

# Step 2: Filter users who ONLY did 'Combo' recharges
combo_data = combo_data.filter(
    (size(col("recharge_type1")) == 1) & (array_contains(col("recharge_type1"), "Combo"))
)

combo_data.show()


# COMMAND ----------

# difference between data and data1 recharge_date and activation_date
data1.printSchema()
data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import datediff
join_data=join_data.withColumn("diff_dates",datediff(col("recharge_date"),col("activation_date")))
join_data.filter(col("row_number")>=1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC in show(truncate='')
# MAGIC | Usage                     | Meaning                            |
# MAGIC | ------------------------- | ---------------------------------- |
# MAGIC | `truncate=True` (default) | Truncates long strings (>20 chars) |
# MAGIC | `truncate=False`          | Shows full data in each column     |
# MAGIC | `truncate=15`             | Truncates at 15 characters         |
# MAGIC

# COMMAND ----------

