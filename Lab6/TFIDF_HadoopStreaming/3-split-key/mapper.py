#!/usr/bin/python3

# Split key -- input tuples are of the form (docid+term, 1) transform to (docid, term)
import sys
import os

for line in sys.stdin:
   import os
   docid, term = line.strip().split("+")
   print('%s\t%s' % (docid, term))
