#!/usr/bin/python3

# LInes look like this:
# sqoop import --connect jdbc:mysql://localhost/world --username training --password training --table City --target-dir

# 10,Tilburg,NLD,Noord-Brabant,193238

import sys
import string

for line in sys.stdin:
    id, name, code, region, pop = line.strip().split(",")
    print('%s\t%s' % (code, pop))
