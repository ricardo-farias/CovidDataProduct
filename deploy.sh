sbt package
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-3-134-112-16.us-east-2.compute.amazonaws.com:~