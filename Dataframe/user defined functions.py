# Databricks notebook source
# MAGIC %md
# MAGIC Used when built-in PySpark functions are not sufficient

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Why the comma?
# MAGIC In Python:
# MAGIC
# MAGIC ("Raju") is just a string in parentheses, not a tuple.
# MAGIC
# MAGIC ("Raju",) is a tuple with one element — the string "Raju".

# COMMAND ----------

from pyspark.sql.functions import udf,col
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("myapp").getOrCreate()
# Sample DataFrame

data=[("Raju",),("Vamshi",),("shiva",),("parimal",),("Pavan",),("Potti_pavan",)]
data_schema=StructType([StructField("name",StringType(),True)])
df=spark.createDataFrame(data,data_schema)
# Custom Python function
def tag_vip(name):
  return "vip" if name.startswith("P") else "Normal"
# Convert Python function to PySpark UDF
#"Hey, the function returns a string, so treat the output column as StringType."
vip_udf=udf(tag_vip,StringType())
# Apply UDF
df=df.withColumn("status",vip_udf(col("name")))
df.show()


# COMMAND ----------

df_vips=df.filter(col("status")=="vip")
print("count",df_vips.count())

# COMMAND ----------

df_vips.groupBy("status").count().show()

# COMMAND ----------

#If you want to count how many people are vip and how many are Normal:
df.groupBy("status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use when / otherwise instead of UDF (more optimized):

# COMMAND ----------

from pyspark.sql.functions import when
df1=df.withColumn("status",when(col("name").startswith("P"),"vip").otherwise("Normal"))
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ✅ Practice 1: UDF to Categorize Age Group
# MAGIC Goal: Create a new column age_group using a UDF.
# MAGIC
# MAGIC | Age Range | Group  |
# MAGIC | --------- | ------ |
# MAGIC | 0-18      | Teen   |
# MAGIC | 19–35     | Young  |
# MAGIC | 36–60     | Adult  |
# MAGIC | 61+       | Senior |
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import udf,col
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("MyAge").getOrCreate()

data=[("Raju",18),("Vamshi",22),("shiva",29),("parimal",35),("Pavan",58),("Potti_pavan",72)]
data_schema=StructType([StructField("name",StringType(),True),
                        StructField("age",IntegerType(),True)])
age_df=spark.createDataFrame(data,data_schema)

def age_range(age):
  if age<=18:
    return "Teen"
  elif age<=35:
    return "Young"
  elif age<=60:
    return "Adult"
  else:
    return "senior"

# Convert Python function to PySpark UDF
age_udf=udf(age_range,StringType())
# Apply UDF
age_df=age_df.withColumn("Group",age_udf(col("age"))).show()



# COMMAND ----------

def mask_name(name):
    return name[0] + "***"

mask_name_udf = udf(mask_name, StringType())

df_masked = df.withColumn("masked_name", mask_name_udf(df["name"]))
df_masked.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC UDF to Check if City is Metro

# COMMAND ----------

def is_metro(city):
    return city in ["Mumbai", "Delhi", "Bangalore", "Kolkata"]

is_metro_udf = udf(is_metro, BooleanType())

df_metro_flag = df.withColumn("is_metro", is_metro_udf(df["city"]))
df_metro_flag.show()


# COMMAND ----------

from pyspark.sql.functions import struct

def name_details(name):
    return (len(name), name.upper())

schema = StructType([
    StructField("length", IntegerType(), True),
    StructField("upper", StringType(), True)
])

name_details_udf = udf(name_details, schema)

df_struct = df.withColumn("name_info", name_details_udf(df["name"]))
df_struct.select("user_id", "name_info.length", "name_info.upper").show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC UDF to Calculate Age from Year of Birth

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

data = [("U001", 1990), ("U002", 1985), ("U003", 2000)]
df = spark.createDataFrame(data, ["user_id", "birth_year"])

def calc_age(year):
    return 2025 - year

age_udf = udf(calc_age, IntegerType())
df = df.withColumn("age", age_udf(col("birth_year")))
df.show()
