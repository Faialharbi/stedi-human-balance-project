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
AWSGlueDataCatalog_node1746132871798 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1746132871798")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746225053937 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1746225053937")

# Script generated for node SQL Query
SqlQuery0 = '''
WITH unique_customers AS (
  SELECT DISTINCT email, shareWithResearchAsOfDate
  FROM stedi_db.customer_trusted
  WHERE shareWithResearchAsOfDate IS NOT NULL
)
SELECT *
FROM stedi_db.accelerometer_landing a
JOIN unique_customers c ON a.user = c.email
WHERE a.timestamp >= c.shareWithResearchAsOfDate;

'''
SQLQuery_node1746273419526 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":AWSGlueDataCatalog_node1746132871798, "c":AWSGlueDataCatalog_node1746225053937}, transformation_ctx = "SQLQuery_node1746273419526")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746273419526, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746273279047", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746273606059 = glueContext.getSink(path="s3://stedi-fai/trusted/accelerometer_trusted_filtered/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746273606059")
AmazonS3_node1746273606059.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted_filtered")
AmazonS3_node1746273606059.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746273606059.writeFrame(SQLQuery_node1746273419526)
job.commit()
