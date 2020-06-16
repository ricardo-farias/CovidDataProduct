#!/bin/bash
sbt assembly
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-52-14-146-106.us-east-2.compute.amazonaws.com:~