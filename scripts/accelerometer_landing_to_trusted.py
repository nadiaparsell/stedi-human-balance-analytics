import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from customer_trusted table (JSON format)
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

# Load data from accelerometer_landing table (JSON format)
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

# Join data on 'user' and 'email' fields
CustomerPrivacyFilter_node = Join.apply(
    frame1=accelerometer_landing_node,
    frame2=customer_trusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node",
)

# Drop sensitive fields from the joined dataset
DropFields_node = DropFields.apply(
    frame=CustomerPrivacyFilter_node,
    paths=[
        "customername",
        "email",
        "phone",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "birthday",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node",
)

# Write the cleaned data to S3 in JSON format
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-nadia2/accelerometer/trusted2/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
