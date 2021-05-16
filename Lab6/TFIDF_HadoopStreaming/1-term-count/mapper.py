#!/usr/bin/python3

import sys
import os

for line in sys.stdin:
   import os
   docid = os.path.splitext(os.path.basename(os.getenv('map_input_file')))[0]
   line = line.strip()
   words = line.strip().split()
   for word in words:
       lowered = word.lower()
       filtered = "".join(filter(lambda c: 97 <= ord(c) <= 122, lowered))
       if len(filtered) > 0:
           print('%s\t%s' % (docid + "+" + filtered, 1))
