README for Spark Streaming

Objective : 
This document explains how the jar file 'assign3.jar' was executed on AWS EMR to monitor an HDFS folder, generate outputs for Tasks A, B, and C, and how the outputs were verified.

Steps to Execute the Job and Verify Output :

Prerequisite :
- Upload the jar file 'assign3.jar' to the cluster master node.

Step 1: Set up the HDFS Directories  
Open Terminal 1 (connected to the master node).  
Create the required directories on HDFS using the following commands:

- hdfs dfs -mkdir -p /input
- hdfs dfs -mkdir -p /output
- hdfs dfs -mkdir -p /checkpoint


Step 2: Run the Spark Job
Open Terminal 2 (connected to the master node).
Run the Spark job using the following spark-submit command:

- spark-submit --class streaming.AssignTask --master yarn --deploy-mode client assign3.jar /input /output /checkpoint


Let the job run for 30-40 seconds to warm up.

Step 3: Upload Data Files to HDFS and Generate Output
In Terminal 1, upload the following data files to the /input directory using these commands while Spark job is running in second terminal:

- hdfs dfs -put Data.txt /input/
- hdfs dfs -put Data2.txt /input/
- hdfs dfs -put Data3.txt /input/

Wait for 40-50 seconds between each upload to allow the Spark job to process the files.
Terminate the spark job using ctrl + C (On Terminal 2).

Step 4: Verify Output Using hdfs dfs -cat
Use Terminal 2 to verify the output using the following commands:

- For Task A Outputs:
    hdfs dfs -cat /output/taskA-001/*
    hdfs dfs -cat /output/taskA-002/*

- For Task B Outputs:
    hdfs dfs -cat /output/taskB-001/*
    hdfs dfs -cat /output/taskB-002/*

- For Task C Outputs:
    hdfs dfs -cat /output/taskC-001/*
    hdfs dfs -cat /output/taskC-002/*

Task C Behavior :
- taskC-001 will generate an initial output, even if no state changes occur.
- taskC-002 and subsequent outputs will only be generated when state changes occur due to the new data files being added to /input (e.g., Data2.txt or Data3.txt).

Summary of Execution Process :
Terminal 1:
- Create HDFS directories and upload data files (Data.txt, Data2.txt, Data3.txt) to /input.
- Upload additional data as needed to trigger state updates for Task C.

Terminal 2:
- Run the Spark job using spark-submit.
- Monitor outputs using hdfs dfs -cat to verify results.