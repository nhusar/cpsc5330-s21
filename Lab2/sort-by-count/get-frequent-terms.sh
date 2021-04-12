#!/bin/bash

# Copy Shakespeare files to HDFS:
# hdfs dfs -copyFromLocal /data/textcorpora/shakespeare-* /input/textcorpora/

# Run the old MapReduce job that produces tab-separated
# (term, count) pairs:
hadoop jar \
    WordCount.jar WordCount \
    /input/textcorpora /term-count

# Use Hadoop Streaming Field Selection and Key Comparator features:
hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -Dmap.output.key.value.fields.spec=1:0 \
    -Dmapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -Dmapred.text.key.comparator.options=-k1,1nr \
    -mapper org.apache.hadoop.mapred.lib.FieldSelectionMapReduce \
    -reducer /bin/cat \
    -input /term-count \
    -output /term-count-desc

# Print the results from HDFS:
hdfs dfs -cat /term-count-desc/part-\* | head -n 10
