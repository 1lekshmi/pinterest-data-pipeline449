# Pinterest Data Pipeline

## Table of Contents

- [Batch Processing](#batch-processing)
  - [Configuring EC2 Kafka Client](#configuring-ec2-kafka-client)
  - [Connecting a MSK cluster to a S3 bucket](#connecting-a-msk-cluster-to-a-s3-bucket)
  - [Configuring an API in API Gateway](#configuring-an-api-in-api-gateway)

## Data

There are three different tables that are processed in this project.These tables are:

- `pinterest_data` - contains data about posts being updated to Pinterest
- `geolocation_data` - contains data about the geolocation of each Pinterest post
- `user_data` - contains data about the user that has uploaded each post

Examples of data from each table:

pinterest_data:

`{'index': 7528, 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf', 'title': 'No Title Data Available', 'description': 'No description available Story format', 'poster_name': 'User Info Error', 'follower_count': 'User Info Error', 'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'is_image_or_video': 'multi-video(story page format)', 'image_src': 'Image src error.', 'downloaded': 0, 'save_location': 'Local save in /data/mens-fashion', 'category': 'mens-fashion'}`

geolocation_data:

`{'ind': 7528, 'timestamp': datetime.datetime(2020, 8, 28, 3, 52, 47), 'latitude': -89.9787, 'longitude': -173.293, 'country': 'Albania'}`

user_data:

`{'ind': 7528, 'first_name': 'Abigail', 'last_name': 'Ali', 'age': 20, 'date_joined': datetime.datetime(2015, 10, 24, 11, 23, 51)}`

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

### Configuring an API in API Gateway

I needed to build an API which will send data to MSK cluster, which in turn will be store in S3 bucket using the connector that I created.

First a resource that allows you to build a PROXY integration for the API was created. Then I created a HTTP `ANY` method for this resource and deployed it. Once the API was set up, I set up the Kafka REST proxy on the EC2 client machine.

To send the data to the S3 bucket, I modified the `user_posting_emulation.py` file.

![](images/topics.png)

The data from each table is uploaded to the corresponding folder as a JSON file

![](images/jsonfiles.png)