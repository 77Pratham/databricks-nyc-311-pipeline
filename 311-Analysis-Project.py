# Databricks notebook source
# Tell our program what tools we need
from pyspark.sql.functions import col, to_timestamp, udf, month, year
from pyspark.sql.types import StringType

# Tell the robots where the data is
# !!! PASTE YOUR FILE PATH FROM STEP 5 HERE !!!
file_location = "/Volumes/workspace/default/311/311_Service_Requests_from_2010_to_Present_20250903.csv"

# Read the data into a smart table (a DataFrame)
df_raw = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(file_location)

print("Data loaded! Let's see what's inside.")
df_raw.printSchema()

# COMMAND ----------

# 1. Pick only the columns we care about and give them simple names
df_cleaned = df_raw.select(
    col("Unique Key").alias("id"),
    col("Created Date").alias("date"),
    col("Complaint Type").alias("complaint"),
    col("Borough").alias("borough")
)

# 2. Tell the robots that the 'date' column is actually a date, not just text
df_cleaned = df_cleaned.withColumn("date", to_timestamp("date", "MM/dd/yyyy hh:mm:ss a"))

# 3. Throw away any rows that are missing important info
df_cleaned = df_cleaned.na.drop(subset=["id", "date", "complaint", "borough"])

# 4. Filter out any complaints from the "Unspecified" borough
df_cleaned = df_cleaned.filter(df_cleaned.borough != "Unspecified")

print("Cleaning complete!")
display(df_cleaned) # display() is a cool Databricks command to see tables

# COMMAND ----------

# This is our custom skill we're teaching the robot
def categorize_complaint(complaint_text):
    complaint_text = complaint_text.lower()
    if 'noise' in complaint_text:
        return 'NOISE'
    elif 'driveway' in complaint_text or 'parking' in complaint_text:
        return 'PARKING/TRAFFIC'
    elif 'heat' in complaint_text or 'water' in complaint_text:
        return 'UTILITIES'
    else:
        return 'OTHER'

# Now, we register this new skill
categorize_udf = udf(categorize_complaint, StringType())

# Use the new skill to create a 'category' column
df_transformed = df_cleaned.withColumn("category", categorize_udf(col("complaint")))

# Let's also pull out the year and month from the date for later
df_transformed = df_transformed.withColumn("year", year(col("date")))
df_transformed = df_transformed.withColumn("month", month(col("date")))


print("Transformation complete! We've made the data smarter.")
display(df_transformed)

# COMMAND ----------

# Create a temporary SQL table so we can ask questions easily
df_transformed.createOrReplaceTempView("complaints_view")

print("Ready for questions! Ask away.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The line above switches this cell to SQL mode
# MAGIC
# MAGIC SELECT
# MAGIC   category,
# MAGIC   COUNT(id) AS number_of_complaints
# MAGIC FROM complaints_view
# MAGIC GROUP BY category
# MAGIC ORDER BY number_of_complaints DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   borough,
# MAGIC   COUNT(id) AS number_of_complaints
# MAGIC FROM complaints_view
# MAGIC GROUP BY borough
# MAGIC ORDER BY number_of_complaints DESC