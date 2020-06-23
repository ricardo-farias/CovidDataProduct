#!/bin/bash

if [ $# -gt 0 ]; then
  location=$1
  sbt assembly
  scp -i EMR-key-pair.pem target/scala-2.11/SparkPractice-assembly-0.1.jar hadoop@"$location":~
  else
    echo "Error: no EMR public master dns found"
    echo "example: ./deploy.sh ec2.example.com"
fi