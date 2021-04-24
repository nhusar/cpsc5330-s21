#!/bin/bash

# Compute TF within each document of the corpus.
# Output is "DOCUMENT_NAME+TERM\tCOUNT"

hdfs dfs -rm -r /textcorpora-df

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /textcorpora \
    -output /textcorpora-df
