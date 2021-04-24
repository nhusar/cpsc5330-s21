#!/bin/bash

# Compute TF within each document of the corpus.
# Output is "DOCUMENT_NAME+TERM\tCOUNT"

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -file ./1-term-count/mapper.py \
    -file ./1-term-count/reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/textcorpora \
    -output /data-output/1-term-count
