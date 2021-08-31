## Apache Flink Application using Glue schema registry

This Apache Flink consumer application to be deployed in Amazon KDA and uses Glue Schema registry.

Steps to create the ClickstreamProcessor-1.0-SNAPSHOT.jar file:

git clone https://github.com/aws-samples/flink-clickstream-processor-msk.git

mvn clean compile assembly:single

Read more in AWS Channel:

[Ingesting, transforming and analyzing real time streaming data using Amazon MSK, Amazon Kinesis Data Analytics, and Amazon Elasticsearch](https://aws.amazon.com/blogs/big-data/part1-build-and-optimize-a-real-time-stream-processing-pipeline-with-amazon-kinesis-data-analytics-for-apache-flink/)


## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.