#! /bin/env bash

sbt package && \
spark-submit \
  --master spark://chstone2:7077 \
  --packages org.apache.hadoop:hadoop-azure:2.7.3,com.microsoft.azure:azure-storage:2.0.0 \
  --class MyApp \
  --name MyApp_$(date +%s) \
  ./target/scala-2.11/searchlogsapp_2.11-1.0.jar
