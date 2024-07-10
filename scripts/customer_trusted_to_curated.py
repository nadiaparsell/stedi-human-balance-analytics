import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script modified for node customer_trusted
customer_trusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/customer/trusted2/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node",
)

# Script modified for node accelerometer_landing
accelerometer_landing_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node = Join.apply(
    frame1=accelerometer_landing_node,
    frame2=customer_trusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node",
)

# Script generated for node Drop Fields
DropFields_node = DropFields.apply(
    frame=CustomerPrivacyFilter_node,
    paths=["user", "x", "y", "z", "timestamp"],
    transformation_ctx="DropFields_node",
)

# Convert DynamicFrame to DataFrame to use Spark API
df = DropFields_node.toDF()

# Drop duplicates
df = df.dropDuplicates()

# Convert back to DynamicFrame
DropDuplicates_node = DynamicFrame.fromDF(
    df, glueContext, "DropDuplicates_node")

# Script generated for node S3 bucket
S3bucket_node = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-nadia2/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node",
)

job.commit()
