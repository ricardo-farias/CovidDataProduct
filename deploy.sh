#!/bin/bash

echo "Building jar....."
sbt assembly
echo "Jar built successfully"
echo "Sending jar to s3......."
aws s3 cp target/scala-2.11/SparkPractice-assembly-0.1.jar s3://emr-configuration-scripts/
echo "Jar sent to s3 successfully"