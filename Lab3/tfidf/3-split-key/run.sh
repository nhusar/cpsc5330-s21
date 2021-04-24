#!/bin/bash

# Compute TF within each document of the corpus.
# Output is "DOCUMENT_NAME\tCOUNT"

hdfs dfs -rm -r /textcorpora-doc-term-count

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper 'tr + "\t"' \
    -input /textcorpora-term-count-per-doc \
    -output /textcorpora-doc-term-count
