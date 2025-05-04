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
AWSGlueDataCatalog_node1746201364404 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1746201364404")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746277288273 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1746277288273")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
  c.sensorReadingTime,
  c.serialNumber,
  c.distanceFromObject
FROM c
JOIN s
  ON c.serialNumber = s.serialNumber

'''
SQLQuery_node1746277354333 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":AWSGlueDataCatalog_node1746201364404, "s":AWSGlueDataCatalog_node1746277288273}, transformation_ctx = "SQLQuery_node1746277354333")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746277354333, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746276867011", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746277479761 = glueContext.getSink(path="s3://stedi-fai/trusted/step_trainer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746277479761")
AmazonS3_node1746277479761.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1746277479761.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746277479761.writeFrame(SQLQuery_node1746277354333)
job.commit()
