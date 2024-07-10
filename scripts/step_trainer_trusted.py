# Import libraries
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

# Load customer curated data
customer_curated_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node",
)

# Load step trainer landing data
step_trainer_landing_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-nadia2/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node",
)

# Join step training landing data and customer curated data
Join_node = Join.apply(
    frame1=step_trainer_landing_node,
    frame2=customer_curated_node,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node",
)

# Drop columns not needed
DropFields_node = DropFields.apply(
    frame=Join_node,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node",
)

# Save step trainer trusted data
S3bucket_node = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-nadia2/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node",
)

job.commit()
