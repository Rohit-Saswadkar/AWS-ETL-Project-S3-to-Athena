# AWS ETL Project: S3 to Athena

## Overview

This project demonstrates an ETL (Extract, Transform, Load) process using AWS Glue, where data is extracted from Amazon S3, transformed using AWS Glue ETL jobs, and loaded back into Amazon S3 in a Parquet format. Finally, the transformed data is queried using AWS Athena.

## Project Structure

1. **Data Source**: Raw data is stored in an S3 bucket (`s-rohit1-epd-project-ip-bucket/input`).
2. **AWS Glue Crawler**: Creates a metadata catalog from the raw data in S3.
3. **AWS Glue ETL Job**: Transforms the raw data.
4. **Transformed Data**: Stored back in S3 (`s-rohit1-epd-project-ip-bucket/output`).
5. **AWS Athena**: Queries the transformed data.

## Prerequisites

- AWS Account
- AWS CLI configured
- Necessary IAM roles and permissions
- Boto3 installed (`pip install boto3`)

## Steps

### 1. Create S3 Buckets

Create two S3 buckets:
- One for input data
- One for storing the transformed output data

### 2. Upload Input Data to S3

Upload your raw data files to the input S3 bucket (`s-rohit1-epd-project-ip-bucket/input`).

### 3. Craete Database in glue data catalog

### 4. Create an AWS Glue Crawler

Create an AWS Glue Crawler to crawl the input S3 bucket and create a database with the metadata.

### 5. Create an AWS Glue ETL Job

Create and configure an AWS Glue ETL job with the following script:

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import logging

logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# Create a handler for CloudWatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('My log message')

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(database="s-rohit1-epd-project-database", table_name="input", transformation_ctx="S3bucket_node1")

logger.info('print schema of S3bucket_node1')
S3bucket_node1.printSchema()

count = S3bucket_node1.count()
print("Number of rows in S3bucket_node1 dynamic frame: ", count)
logger.info('count for frame is {}'.format(count))

# Apply Mapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("ordernumber", "bigint", "new_ordernumber", "bigint"),
        ("quantityordered", "bigint", "new_quantityordered", "bigint"),
        ("priceeach", "double", "new_priceeach", "double"),
        ("orderlinenumber", "bigint", "new_orderlinenumber", "bigint"),
        ("sales", "double", "new_sales", "double"),
        ("orderdate", "string", "new_orderdate", "string"),
        ("status", "string", "new_status", "string"),
        ("qtr_id", "bigint", "new_qtr_id", "bigint"),
        ("month_id", "bigint", "new_month_id", "bigint"),
        ("year_id", "bigint", "new_year_id", "bigint"),
        ("productline", "string", "new_productline", "string"),
        ("msrp", "bigint", "new_msrp", "bigint"),
        ("productcode", "string", "new_productcode", "string"),
        ("customername", "string", "new_customername", "string"),
        ("phone", "string", "new_phone", "string"),
        ("addressline1", "string", "new_addressline1", "string"),
        ("addressline2", "string", "new_addressline2", "string"),
        ("city", "string", "new_city", "string")
    ],
    transformation_ctx="ApplyMapping_node2"
)

# Convert dynamic dataframe into spark dataframe
logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')
spark_data_frame=ApplyMapping_node2.toDF()

# Apply spark where clause
logger.info('filter rows with where priceeach is not null')
spark_data_frame_filter = spark_data_frame.where("priceeach is NOT NULL")

spark_data_frame_filter.show()

logger.info('convert spark dataframe into table view product_view. so that we can run sql ')
spark_data_frame_filter.createOrReplaceTempView("sales_view")

logger.info('create dataframe by spark sql ')
product_sql_df = spark.sql("SELECT new_city, new_year_id, count(ordernumber) as order_count,sum(new_quantityordered) as total_qty, sum(new_sales) as total_sales FROM product_view group by new_city ")

logger.info('display records after aggregate result')
product_sql_df.show()

# Convert the data frame back to a dynamic frame
logger.info('convert spark dataframe to dynamic frame ')
dynamic_frame = DynamicFrame.fromDF(product_sql_df, glueContext, "dynamic_frame")

logger.info('dynamic frame uploaded in bucket s-rohit1-epd-project-ip-bucket/output in parquet format ')
# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="s3", format="glueparquet", connection_options={"path": "s3://s-rohit1-epd-project-ip-bucket/output/", "partitionKeys": []}, transformation_ctx="S3bucket_node3")

logger.info('etl job processed successfully')
job.commit()
```

### 6. Create Athena Table

Use AWS Athena to create a table and query the transformed data:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS mydatabase.product_data (
  new_city STRING,
  new_year_id BIGINT,
  order_count BIGINT,
  total_qty BIGINT,
  total_sales DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://s-rohit1-epd-project-ip-bucket/output/';
```

### 7. Query Data Using Athena

Run SQL queries on the data stored in the Athena table to generate insights.

## Conclusion

This project demonstrates how to set up an ETL pipeline using AWS Glue, transforming data stored in S3, and querying the transformed data using Athena.

## Notes

- Ensure that all IAM roles and policies have the necessary permissions.
- Adjust paths and parameters as needed to fit your specific requirements.

## Author

Rohit Rajendra Saswatkar

---

Feel free to modify this README to better match your specific project setup and requirements.
