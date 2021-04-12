#!/usr/bin/env python3

import re
import sys


# 2015-10-18 18:01:47,978 INFO [main] ...
_RE_MINUTE_SEVERITY = re.compile(
    r"^(\d\d\d\d-\d\d-\d\d \d\d:\d\d):\d\d,\d\d\d (INFO|WARN|ERROR|FATAL) ")

for ln in sys.stdin:
    match = _RE_MINUTE_SEVERITY.match(ln)
    if match:
        print("%s\t%s" % (match.group(1), match.group(2)))
