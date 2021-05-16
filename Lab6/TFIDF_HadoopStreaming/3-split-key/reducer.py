#!/usr/bin/python3

# This is a "null reducer" -- mapper gives us (docid, term) and we pass it on unchanged.

import sys

for line in sys.stdin:
    print(line.strip())
