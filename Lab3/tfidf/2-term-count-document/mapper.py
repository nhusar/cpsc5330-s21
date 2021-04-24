#!/usr/bin/env python3

import sys

# Read line "doc+term\tcount", output "doc\tcount"
for ln in sys.stdin:
    (doc, rest) = ln.strip().split("+", 2)
    (_term, count_str) = rest.split("\t")
    print("%s\t%s" % (doc, count_str))
