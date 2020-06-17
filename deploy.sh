#!/bin/bash
sbt assembly
FILE=target.zip
if test -f "$FILE"; then
    rm -rf $FILE
fi
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-18-223-237-225.us-east-2.compute.amazonaws.com:~