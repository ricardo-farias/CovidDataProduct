sbt package
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-18-188-252-197.us-east-2.compute.amazonaws.com:~