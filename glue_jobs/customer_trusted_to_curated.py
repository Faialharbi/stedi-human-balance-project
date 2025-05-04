import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746134067318 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted_filtered", transformation_ctx="AWSGlueDataCatalog_node1746134067318")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746134066904 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1746134066904")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM stedi_db.customer_trusted
WHERE shareWithResearchAsOfDate IS NOT NULL

'''
SQLQuery_node1746134097831 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":AWSGlueDataCatalog_node1746134067318, "a":AWSGlueDataCatalog_node1746134066904}, transformation_ctx = "SQLQuery_node1746134097831")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746134097831, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746133886869", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746134175481 = glueContext.getSink(path="s3://stedi-fai/curated/customer/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746134175481")
AmazonS3_node1746134175481.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
AmazonS3_node1746134175481.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746134175481.writeFrame(SQLQuery_node1746134097831)
job.commit()
