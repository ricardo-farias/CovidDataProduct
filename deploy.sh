#!/bin/bash

if $# -gt 0; then
  location=$1

  sbt assembly
  FILE=target.zip
  if test -f "$FILE"; then
      rm -rf $FILE
  fi
  zip -r target.zip target
  scp -i EMR-key-pair.pem target.zip hadoop@"$location":~

  else
    echo "Error: no EMR public master dns found"
    echo "example: ./deploy.sh ec2.example.com"
fi