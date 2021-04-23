#!/usr/bin/python3
import sys

# Input like AFG	237500

current_country = None
current_sum = 0
current_count = 0

for line in sys.stdin:
    line = line.strip()
    country, pop = line.split('\t', 1)
    pop = int(pop)

    if current_country == country:
        current_sum += pop
        current_count += 1
    else:
        if current_country:
            print('%s\t%f' % (current_country, float(current_sum)/current_count))
        current_country = country
        current_sum = pop
        current_count = 1

if current_country == country:
    print('%s\t%f' % (current_country, float(current_sum)/ current_count))
