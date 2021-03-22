# Description  
MapReduce task to aggregate device utilization logs in given time interval

# Required tools  
- JDK (version 7 or later)
- git
- maven
- docker

# Download docker image and create short tag
```
docker pull vladimirnemo/hadoop-single-node:latest
docket image tag vladimirnemo/hadoop-single-node:latest hadoop-single-node:latest 
``` 

# Build application and start container
```
cd Mapred
./startContainer.sh
```
startContainer.sh will compile jar with dependencies, generate test input files, start docker container, copy all necessary files to container and open a bash interactive session  
 
NameNode UI will be available at http://localhost:50070 
# Running the application
The following commands should be executed in opened container shell

- copy data to hdfs
 
HINT: You have to wait a bit before run hdfs commands because after start NameNode is in safe mode
```
if $($HADOOP_PREFIX/bin/hdfs dfs -test -d input); then $HADOOP_PREFIX/bin/hdfs dfs -rm -r input; fi
if $($HADOOP_PREFIX/bin/hdfs dfs -test -d output); then $HADOOP_PREFIX/bin/hdfs dfs -rm -r output; fi
$HADOOP_PREFIX/bin/hdfs dfs -put /root/data/input input
```

- run MapReduce job

```
$HADOOP_PREFIX/bin/yarn jar /root/MapRed-1.0-SNAPSHOT-jar-with-dependencies.jar -files /root/data/mapping input output 3 m1
```

- show output

```
$HADOOP_PREFIX/bin/hdfs dfs -ls output
$HADOOP_PREFIX/bin/hdfs dfs -libjars /root/MapRed-1.0-SNAPSHOT-jar-with-dependencies.jar -text output/part-r-00000
```

- stop container  
To stop the container you have to run the following command from host shell

```
./stopContainer.sh
```

Screenshots are available [here](./screenshots/README.md)