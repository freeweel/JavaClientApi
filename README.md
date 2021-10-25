# JavaClientApi
 Java Client API including DMSDK and DHF Writer

This project is a fully functional Java application that performs the following:
1. Extracts data from AWS S3 using AWS libraries
2. Streams S3 data into MarkLogic Data Hub using MarkLogic Java Client API and Data Movement SDK (DMSDK)
3. Also allows launching a data hub flow after data is loaded

** NOTE: This project uses Java JDK 1.8 and Gradle 6.x **

To run the examples:
1. Download code 
2. Ensure you have gradle and JDK set up
4. Build code using gradle
5. You will need a working MarkLogic instance as well as files in an S3 bucket on AWS
6. Open and read the src/main/resources/AccessKeys.README
7. Create AccessKeys.secret file in the same directory using your AWS and MarkLogic credentials and environment settings
8. Run the following main program for loading data from S3 to MarkLogic
      - src/main/java/main/LoadS3ToMarkLogic.java

# Run a Data Hub Flow

** NOTE: following main program assumes you have set up a data hub and have created some flows within it **

Run the following main programe for starting a data hub flowl
  - src/main/java/main/RunDataHubFlow.java
