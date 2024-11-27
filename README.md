# emr-iceberg-tags-demo


![output](https://github.com/user-attachments/assets/48a14b09-babe-42da-a291-771f771d71ca)

### Steps 
#### step 1: Create job.py
```

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime

# Initialize SparkSession with Iceberg and AWS Glue configurations
print("Initializing Spark Session with Iceberg and Glue configurations...")
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.dev", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.dev.warehouse", "s3://XXXX/warehouse/") \
    .config("spark.sql.catalog.dev.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.dev.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.sql.catalog.dev.s3.write.tags.write-tag-name", "created") \
    .config("spark.sql.catalog.dev.s3.delete.tags.delete-tag-name", "deleted") \
    .config("spark.sql.catalog.dev.s3.delete-enabled", "false") \
    .getOrCreate()

# Define the Iceberg table name
table_name = "people_iceberg_delete_enabled_false"
print(f"Working with table: {table_name}")

# Define the schema for the records
print("Defining schema for the DataFrame...")
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("create_ts", StringType(), True)
])

# Create initial records to write to the table
print("Creating initial records...")
records = [
    (1, 'John', 25, 'NYC', '2023-09-28 00:00:00'),
    (2, 'Emily', 30, 'SFO', '2023-09-28 00:00:00'),
    (3, 'Michael', 35, 'ORD', '2023-09-28 00:00:00'),
    (4, 'Andrew', 40, 'NYC', '2023-10-28 00:00:00'),
    (5, 'Bob', 28, 'SEA', '2023-09-23 00:00:00'),
    (6, 'Charlie', 31, 'DFW', '2023-08-29 00:00:00')
]

# Create DataFrame with the schema and records
print("Creating DataFrame from records...")
df = spark.createDataFrame(records, schema)
df.show()

# Write the DataFrame to Iceberg table, partitioned by 'city'
print(f"Writing DataFrame to Iceberg table: {table_name} (partitioned by 'city')...")
df.writeTo(f"dev.default.{table_name}") \
    .partitionedBy("city") \
    .createOrReplace()

# Verify data written to the table
print(f"Data written to {table_name}. Fetching data from the table...")
spark.sql(f"SELECT * FROM dev.default.{table_name}").show()

# Display Iceberg history for the table
print(f"Fetching table history for {table_name}...")
spark.sql(f"SELECT * FROM dev.default.{table_name}.history").show()

# Append new records to the table
print("Appending new records to the table...")
append_records = [
    (7, 'Diana', 29, 'MIA', '2023-10-28 00:00:00'),
    (8, 'Evan', 34, 'NYC', '2023-11-01 00:00:00')
]
append_df = spark.createDataFrame(append_records, schema)
append_df.show()

# Write the appended records to the table
print(f"Appending new records to {table_name}...")
append_df.writeTo(f"dev.default.{table_name}").append()

# Fetch updated table data and history
print("Fetching updated data and history after appending...")
spark.sql(f"SELECT * FROM dev.default.{table_name}").show()
spark.sql(f"SELECT * FROM dev.default.{table_name}.history").show()

# Delete records where city = 'NYC'
print("Executing DELETE operation: Removing records where city = 'NYC'...")
spark.sql(f"DELETE FROM dev.default.{table_name} WHERE id = '4'")
spark.sql(f"DELETE FROM dev.default.{table_name} WHERE id = '8'")

# Verify the data and history after DELETE operation
print("Fetching data and history after DELETE operation...")
spark.sql(f"SELECT * FROM dev.default.{table_name}").show()
spark.sql(f"SELECT * FROM dev.default.{table_name}.history").show()

# Expire snapshots, retaining the last 2 snapshots
print("Expiring snapshots...")
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
retain_last = 2
expire_snapshots_query = f"""
CALL dev.system.expire_snapshots(
    'default.{table_name}', 
    TIMESTAMP '{current_time}',
    {retain_last}
)
"""
print("Executing snapshot expiration query...")
spark.sql(expire_snapshots_query).show()

# Append new records to the table
print("Appending new records to the table...")
append_records = [
    (9, 'Soumil', 29, 'NYC', '2023-10-28 00:00:00'),
    (10, 'Nitin', 34, 'NYC', '2023-11-01 00:00:00')
]

append_df = spark.createDataFrame(append_records, schema)
append_df.show()
print(f"Appending new records to {table_name}...")
append_df.writeTo(f"dev.default.{table_name}").append()

print("All operations completed successfully!")
```

#### step 2: push the script to S3 
```
export APPLICATION_ID="XXX"
export AWS_ACCOUNT="XX"
export BUCKET="xXX"
export IAM_ROLE="arn:aws:iam::${AWS_ACCOUNT}:role/EMRServerlessS3RuntimeRole"
aws s3 cp /Users/sshah/IdeaProjects/workshop/aws-scripts/demo/job.py s3://$BUCKET/jobs/job.py
```

<img width="751" alt="image" src="https://github.com/user-attachments/assets/5ea49f30-8855-48f5-a6f9-52bf23230338">


#### Step 3: Submit job
```

aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "iceberg-cost-optimizations" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://'"$BUCKET"'/jobs/job.py",
            "sparkSubmitParameters": "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev.warehouse=s3://'"$BUCKET"'/warehouse --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.catalog.job_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.sources.partitionOverwriteMode=dynamic --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'"$BUCKET"'/logs/"
            }
        }
    }'
```


#### Step 4: verfiy tags 
```
import boto3
import subprocess

# Initialize a session using Amazon S3
s3_client = boto3.client('s3')

bucket_name = 'XXX'
prefix = 'warehouse/default.db/people_iceberg_delete_enabled_false/data/'
# Step 1: List all objects in the specified S3 path
def list_s3_objects(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        return [item['Key'] for item in response['Contents']]
    else:
        print("No objects found.")
        return []

# Step 2: Get object tags for each object
def get_object_tags(bucket, key):
    response = s3_client.get_object_tagging(Bucket=bucket, Key=key)
    tags = response['TagSet']
    return tags

# Get list of objects in the specified path
objects = list_s3_objects(bucket_name, prefix)

# Loop over each object and get its tags
for obj_key in objects:
    print(f"Fetching tags for: {obj_key}")
    tags = get_object_tags(bucket_name, obj_key)
    print(f"Tags for {obj_key}: {tags}")


```

<img width="1574" alt="image" src="https://github.com/user-attachments/assets/46deaa6b-baf3-4944-a0cf-707cba71e03e">


#### Step 5: setup life cycle policy 
```
aws s3api put-bucket-lifecycle-configuration --bucket XXX --lifecycle-configuration '{
  "Rules": [
    {
      "ID": "Move deleted data to IF and Glacier",
      "Filter": {
        "Tag": {
          "Key": "delete-tag-name",
          "Value": "deleted"
        }
      },
      "Status": "Enabled",
      "Transitions": [
        {
          "Days": 0,
          "StorageClass": "INTELLIGENT_TIERING"
        },
        {
          "Days": 30,
          "StorageClass": "GLACIER"
        }
      ],
      "Expiration": {
        "Days": 365
      }
    }
  ]
}'

```


### happy learning 
