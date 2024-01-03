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

## 2. Batch processing

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
* The Kafka topics were created using the following string in the EC2 environment:

* 
