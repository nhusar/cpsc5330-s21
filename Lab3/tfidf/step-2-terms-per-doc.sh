#!/bin/bash

# Count the number of terms in each document.
# Input is "DOCUMENT_NAME+TERM\tCOUNT", output is "DOCUMENT_NAME\tCOUNT"

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -file ./2-term-count-document/mapper.py \
    -file ./2-term-count-document/reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data-output/1-term-count \
    -output /data-output/2-term-count-document
