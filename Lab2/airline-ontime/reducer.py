#!/usr/bin/env python3
"""
Hadoop streaming script:

hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper /bin/cat \
    -reducer reducer.py \
    -input /airline-sqoop \
    -output /airline-ontime
"""

import sys
import collections


def _print_stats(key, stats):
    if key is not None:
        print("%s\t%f\t%f\t%f" % (
            key, stats["min"], stats["max"],
            stats["sum"] / stats.get("count", 1.0)))


cur_key = None
stats = {}

for ln in sys.stdin:
    (key, val) = ln.strip().split('\t', 2)
    if val == "null":
        continue
    val = float(val)
    if key != cur_key:
        _print_stats(cur_key, stats)
        cur_key = key
        stats = collections.defaultdict(float)
    stats["min"] = min(val, stats.get("min", float("inf")))
    stats["max"] = max(val, stats.get("max", float("-inf")))
    stats["sum"] += val
    stats["count"] += 1.0

_print_stats(cur_key, stats)
