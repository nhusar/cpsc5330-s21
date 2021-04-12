#!/usr/bin/env python3

import sys
import collections


_KEYS = ["TOTAL", "INFO", "WARN", "ERROR", "FATAL"]

def _print_stats(key, stats):
    if key is not None:
        print("%s\t%s" % (key, "\t".join([str(stats[k]) for k in _KEYS])))


cur_key = None
stats = {}

for ln in sys.stdin:
    (key, val) = ln.strip().split('\t', 2)
    if key != cur_key:
        _print_stats(cur_key, stats)
        cur_key = key
        stats = collections.defaultdict(int)
    stats[val] += 1
    stats["TOTAL"] += 1

_print_stats(cur_key, stats)
