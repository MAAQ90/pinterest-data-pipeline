# Emulation of Pinterest Data Pipelines

## Table of contents:
1. About
2. Batch processing
3. Stream processing
4. File structure
5. License information

## 1. About

Pinterest produces billions of data points daily through user interaction. This project aimed to build a cloud-based end-to-end data processing pipelines using Pinterest's data generation emulator. The system was capable of handling both batch and streams of data. The following tools were utlised in order to do so:

1. AWS S3 for data storage
2. AWS MSK for stream processing
3. Databricks (Python environment) for data transformation
4. AWS MWAA for workflow orchestration
5. AWS API Gateway for data ingestion

## 2. Batch processing using Kafka

### 2.1. Configuration of EC2 Kafka client

Series of steps were required to setup the EC2 Kafka client for this project:

* Generation of key-pair file to allow connection with the EC2 instance.
* Installation of JAVA and downloading Kafka (2.12-2.8.1).
```commandline
sudo yum install java-1.8.0
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar -xzf kafka_2.12-2.8.1.tgz
```
* The MSK cluster was already setup. Therefore, only the installation of IAM MSK authentication package on the client EC2 machine was required.
```commandline
cd /home/ec2-user
touch kafka_2.12-2.8.1/bin/client.properties
vi kafka_2.12-2.8.1/bin/client.properties
```

### 2.2. Create Kafka topics

* In order to create Kafka topics, the following information regarding the MSK clusters were extracted from AWS console:
  * Bootstrap servers string
  * Plaintext Apache Zookeeper connection string
* The Kafka topics were created using the following string in the EC2 environment, in the given format:
  * <user-id>.pin for Pinterest data
  * <user-id>.geo for Geolocation data
  * <user-id>.user for Users data
 ```commandline
./kafka-topics.sh --bootstrap-server <BootstrapServerString> --command-config client.properties --create --topic <user-id>.pin
./kafka-topics.sh --bootstrap-server <BootstrapServerString> --command-config client.properties --create --topic <user-id>.geo
./kafka-topics.sh --bootstrap-server <BootstrapServerString> --command-config client.properties --create --topic <user-id>.user
```

### 2.3. Connect the MSK cluster to S3 bucket

* Make note of your S3 bucket name: user-<user-id>-bucket.
* Download Confluent.io Amazon S3 connector and copy it to <your-s3-bucket>, using the following: 
```commandline
- sudo -u ec2-user -i
- mkdir kafka-connect-s3 && cd kafka-connect-s3
- wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
- aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<your-s3-bucket>/kafka-connect-s3/
 ```
* Using the AWS console (MSK Connect), create the custom plug-in as: <user-id>-plugin
* Then create a connector in MSK Connect: <user-id>-connector, and make sure that you use correct S3 bucket during configuration (user-<user-id>-bucket).

### 2.4. Configure API using API Gateway

In this step, Kafka REST proxy integration method for the API was configured:

* Navigate to API Gateway on you AWS console and create a resource for PROXY integration.
* Create an HTTP 'ANY' method, and copy 'PublicDNS' of your EC2 machines to 'Endpoint URL'.
* Deploy the API and note the 'Invoke URL'.
* Install the Confluent package using the following commands on your EC2 client:
```commandline
cd /home/ec2-user/
sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz
tar -xvzf confluent-7.2.0.tar.gz 
```
* Modify the 'kafka-rest.properties' file to allow the REST proxy to perform IAM authentication, by adding the following properties:
```commandline
# Sets up TLS for encryption and SASL for authN.
client.security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
client.sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=<your-aws-RoleArn>;

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler 
```
* Start the REST proxy on the EC2 client machine:
```commandline
./confluent-7.2.0/bin/kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
```

### 2.5. Sparks on Databricks

In this step, Databricks account is setup, the S3 bucket 'user-<user-id>-bucket' is mounted, data is cleaned, and queries are made.
* The required libraries for sparks data processing are imported using the following code:
```python
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, ArrayType, DoubleType
from pyspark.sql.window import Window
import urllib
```
* Mounting of S3 bucket is required to read the pinterest, geolocation, and users data, into sparks dataframe, named as df_pin, df_geo, and df_user respectively.\
Refer to code: https://github.com/MAAQ90/pinterest-data-pipeline2/blob/main/databricks_notebooks/s3-mount.ipynb
* The data cleaning is then carried on all three data frames.\
Refer to code: https://github.com/MAAQ90/pinterest-data-pipeline2/blob/main/databricks_notebooks/data_clean_and_queries.ipynb

## 3. Stream processing using AWS Kinesis

Create 3 data streams using 'Kinesis Data Streams', in the following format, with capacity mode set to 'On-demand':
* streaming-<user-id>-pin for pinterest data stream
* streaming-<user-id>-geo for geolocation data stream
* streaming-<user-id>-user for users data stream

### 3.1. Configure an API with Kinesis proxy integration

Further actions need to be carried out on previously created REST API using AWS API Gateway. Retrieve the ARN of access role (to Kinesis) and use this when setting up the Execution role for the integration point of all the methods being created. The API invokes the following actions:
* List streams in Kinesis
* Create, describe, and delete streams in Kinesis
* Add records to streams in Kinesis

### 3.2. Send data to and read data from the Kinesis streams in Databricks

* A python is created to send data in to the Kinesis streams.
Refer to code: https://github.com/MAAQ90/pinterest-data-pipeline2/blob/main/python_data/user_posting_emulation_stream.py
* The code that uses PySparks capabilites is written to ingest data into Kinesis Data Streams:
Refer to code: 


### 3.3. Transform Kinesis streams in Databricks

### 3.4. Write the streaming data to Delta Tables
