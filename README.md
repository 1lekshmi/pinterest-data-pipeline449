# Pinterest Data Pipeline

## Table of Contents

- [Batch Processing](#batch-processing)
  - [Configuring EC2 Kafka Client](#configuring-ec2-kafka-client)
  - [Connecting a MSK cluster to a S3 bucket](#connecting-a-msk-cluster-to-a-s3-bucket)

## Batch Processing

### Configuring EC2 Kafka Client

Created a key pair file using the key pair associated with my EC2 instance. This file was then used to connect to the EC2 instance using an SSH client

Since the AWS account provided for this project already had access to an IAM authenticated MSK cluster, there was no need to create my own cluster. I still needed to install the appropriate packages to the EC2 client machine in order to connect to this cluster. Kafka and IAM MSK authentication package was installed to the EC2 client machine. Once it was installed, I configured the Kafka client to use AWS IAM authentication to the cluster. Once everything was installed, I then created three Kafka topics, `<user_id>.pin` for Pinterest posts data, `<user_id>.geo` for post geolocation data and `<user_id>.user` for post user data

### Connecting a MSK cluster to a S3 bucket

S3 bucket and an IAM role that allows you to write to the bucket have already been configured.

On the EC2 client machine, Confluent.io Amazon S3 connector was downloaded and then copied to the S3 bucket
![](images/bucket.png)

Created a custom plugin
![](images/msk_plugin.png)

and a connector
![](images/msk_connector.png)
