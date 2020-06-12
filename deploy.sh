sbt assembly
zip -r target.zip target
scp -i EMR-key-pair.pem target.zip hadoop@ec2-3-19-242-232.us-east-2.compute.amazonaws.com:~