#!/bin/bash

# Compute TF within each document of the corpus.
# Output is "DOCUMENT_NAME\tCOUNT"

hdfs dfs -rm -r /textcorpora-doc-size

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /textcorpora-term-count-per-doc \
    -output /textcorpora-doc-size
