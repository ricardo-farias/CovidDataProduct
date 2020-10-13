#!/bin/bash

echo "Building jar....."
sbt assembly
echo "Jar built successfully"
echo "Sending jar to s3......."
aws s3 cp target/scala-2.11/CovidDataProduct-assembly-0.1.jar s3://data-mesh-poc-ricardo-emr-jar-bucket/
echo "Jar sent to s3 successfully"