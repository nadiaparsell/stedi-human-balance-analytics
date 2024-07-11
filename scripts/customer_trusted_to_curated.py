# Import libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load customer trusted data
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

# Load accelerometer landing data
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

# Join accelerometer landing and customer trusted
CustomerPrivacyFilter_node = Join.apply(
    frame1=accelerometer_landing_node,
    frame2=customer_trusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node",
)

# Drop columns not needed
DropFields_node = DropFields.apply(
    frame=CustomerPrivacyFilter_node,
    paths=["user", "x", "y", "z", "timestamp"],
    transformation_ctx="DropFields_node",
)

# Convert DynamicFrame to dataframe so i can drop duplicates
df = DropFields_node.toDF()
df = df.dropDuplicates()
DropDuplicates_node = DynamicFrame.fromDF(
    df, glueContext, "DropDuplicates_node")

# Save customer curated data
S3bucket_node = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node,
    connection_type="s3",
    format="json",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    connection_options={
        "path": "s3://stedi-lake-house-nadia2/customer/curated/",
    },
    transformation_ctx="S3bucket_node",
)

job.commit()
