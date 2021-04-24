#!/usr/bin/env python3

import sys


def _print_term(prev_term, prev_count):
    if prev_term is not None:
        print("%s\t%d" % (prev_term, prev_count))


prev_term = None
prev_doc = None
prev_count = 0

for ln in sys.stdin:
    (term, doc) = ln.strip().split("\t")
    if term != prev_term:
        _print_term(prev_term, prev_count)
        prev_term = term
        prev_doc = doc
        prev_count = 1
    if doc != prev_doc:
        prev_doc = doc
        prev_count += 1

_print_term(prev_term, prev_count)
