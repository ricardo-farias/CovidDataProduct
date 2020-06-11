sbt package
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-18-188-165-23.us-east-2.compute.amazonaws.com:~