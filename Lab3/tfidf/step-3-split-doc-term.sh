#!/bin/bash

# Split doc+term into two separate fields.
# Input is "DOCUMENT_NAME+TERM\tCOUNT", output is "DOCUMENT_NAME\tTERM\tCOUNT"

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper 'tr + "\t"' \
    -input /data-output/1-term-count \
    -output /data-output/3-split-doc-term
