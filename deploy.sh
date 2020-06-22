#!/bin/bash
sbt assembly
FILE=target.zip
if test -f "$FILE"; then
    rm -rf $FILE
fi
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-3-22-70-62.us-east-2.compute.amazonaws.com:~