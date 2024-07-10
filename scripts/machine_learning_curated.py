import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load accelerometer data into DataFrame
accelerometer_trusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/accelerometer/trusted2/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node",
).toDF()

# Load step trainer data into DataFrame
step_trainer_trusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node",
).toDF()

# Register the DataFrames as temporary views
accelerometer_trusted_node.createOrReplaceTempView("accelerometer_trusted")
step_trainer_trusted_node.createOrReplaceTempView("step_trainer_trusted")

# Perform the SQL join operation
joined_df = spark.sql("""
SELECT * 
FROM accelerometer_trusted 
JOIN step_trainer_trusted 
ON accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
""")

# Drop unnecessary fields if required (example here: 'user')
joined_df = joined_df.drop("user")

# Convert the joined DataFrame back to a DynamicFrame
joined_dynamic_frame = DynamicFrame.fromDF(
    joined_df, glueContext, "joined_dynamic_frame")

# Write the result to S3
glueContext.write_dynamic_frame.from_options(
    frame=joined_dynamic_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-nadia2/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

# Commit the job
job.commit()
