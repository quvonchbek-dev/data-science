from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()

jobDf = spark.read.parquet('data/jobs.parquet')
empDf = spark.read.parquet('data/employees.parquet')
df = empDf.join(jobDf, empDf.job_id == jobDf.job_id, "inner") \
    .filter("job_title='Sales Representative'").sort("salary", "first_name")
df.select("first_name", "last_name", "salary", "job_title").show(11, truncate=False)
