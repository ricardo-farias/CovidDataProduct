### Background
The purpose of this project is to create an example of a working data mesh in AWS.

### Tech Stack

- AWS EMR - Data ETL
- AWS Glue - Data catalog
- AWS Athena - Data analytics
- AWS S3 - Data storage
- Terraform - AWS resource manager
- Scala Spark - Development tools
- CircleCi - Continuous deployment


### Steps to Recreate

 1. Run the terraform project https://github.com/ricardo-farias/TerraformDataMesh
    
    - The terraform project will create the necessary infrastructure to run this spark application.
 2. Once the terraform app has finished creating all the resources, 
    retrieve the EMR master public dns, which will be outputted by terraform.
 3. ssh into the EMR cluster, and set aws profile access key and secret key.
    - To do this run this command:
      ```shell script
      ssh -i ~/placeSomeKeyHere.pem hadoop@place-master-public-dns-here
      nano .aws/credentials
      ````
      This will create a text file, inside the text file write this:
      ```
      [profile sparkapp]
      aws_access_key_id=?????
      aws_secret_access_key=????
      ```
      make sure to substitute the ? for your appropriate keys.
 4. Run the deploy.sh script to build and deploy the spark app to EMR.
    ```shell script
    ./deploy place-master-public-dns-here
    ```
 5. In EMR, unzip and run the deployed app by running this command:
    ```shell script
    unzip target.zip
    spark-submit --class com.ricardo.farias.App target/scala-2.11/SparkPractice-assembly-0.1.jar
    ``` 
If the spark application runs successfully, then aws athena, glue, and s3 will be populated with data.