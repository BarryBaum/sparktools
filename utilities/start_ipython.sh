#!/usr/bin/env bash

if [ "$ENV" = "awsprod" ]; then
  readonly CASSANDRA="spark.cassandra.connection.host=xxx.xxx.xxx.xxx" # For Prod comma delimited no spaces
elif [ "$ENV" = "awsqa" ]; then
  readonly CASSANDRA="spark.cassandra.connection.host=xxx.xxx.xxx.xxx" # For QA
else
  readonly CASSANDRA="spark.cassandra.connection.host=xxx.xxx.xxx.xxx"  # For Dev
fi
export PYSPARK_DRIVER_PYTHON=ipython

#pyspark params below, works for yarn and local mode 
pyspark \
        --master "yarn" \
        --driver-memory "4g" \
        --driver-cores "4" \
        --num-executors "2" \
        --executor-cores "5" \
        --executor-memory "45g" \
        --packages datastax:spark-cassandra-connector:2.0.0-s_2.11,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2 \
        --conf "spark.yarn.executor.memory.overhead=10g" \
        --conf "spark.driver.maxResultSize=36g" \
        --conf "spark.eventLog.enabled=true" \
        --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-UseLoopPredicate -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:-ResizePLAB" \
        --conf "$CASSANDRA"
