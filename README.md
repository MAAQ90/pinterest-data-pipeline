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

## 2. Batch processing usinf Kafka

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

* Make note of your S3 bucket name <your-s3-bucket>.
* Download Confluent.io Amazon S3 connector and copy it to <your-s3-bucket>, using the following: 
 ```commandline
- sudo -u ec2-user -i
- mkdir kafka-connect-s3 && cd kafka-connect-s3
- wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
- aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<your-s3-bucket>/kafka-connect-s3/
 ```

### 2.4. Configure API using API Gateway

### 2.5. Sparks on Databricks

## 3. Stream processing using AWS Kinesis

### 3.1. Configure an API with Kinesis proxy integration

### 3.2. Send data to and read data from the Kinesis streams in Databricks

### 3.3. Transform Kinesis streams in Databricks

### 3.4. Write the streaming data to Delta Tables
