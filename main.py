from pyspark.sql import SparkSession

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, split

spark = SparkSession.builder.master('local').getOrCreate()

department = spark.read.parquet('data/department.parquet')
locations = spark.read.parquet('data/locations.parquet')
employees = spark.read.parquet('data/employees.parquet')
job_history = spark.read.parquet('data/job_history.parquet')

job_history.join(employees, ["employee_id"]).show(200, False)
