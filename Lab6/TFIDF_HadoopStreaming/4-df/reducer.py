#!/usr/bin/python3

# DF Reducer -- mapper gives us (token, docid)
# We produce (token, #docs) where #docs is the number of unique doc IDs

import sys

current_token = None
current_docs = {}
token = None

for line in sys.stdin:
    line = line.strip()
    token, docid = line.split('\t', 1)

    if current_token == token:
        current_docs[docid]=1
    else:
        if current_token:
            print('%s\t%s' % (current_token, len(list(current_docs.keys()))))
        current_token = token
        current_docs = {docid: 1}

if current_token == token:
    print('%s\t%s' % (current_token, len(list(current_docs.keys()))))
