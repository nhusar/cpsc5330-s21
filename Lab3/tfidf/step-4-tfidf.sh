#!/bin/bash

# Compute teh number of documents containing each term.
# Output is "TERM\tNUM_DOCS"

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -file ./4-df/mapper.py \
    -file ./4-df/reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/textcorpora \
    -output /data-output/4-df
