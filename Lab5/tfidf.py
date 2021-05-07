#!/usr/bin/env python3

import re
import sys
import collections

import boto3
from boto3.dynamodb.conditions import Key


class SearchRank:

    _RE_WORDS = re.compile(r"[a-z]+")

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('HanksTFIDF')

    def query_term(self, term, doc_ranks):
        response = self.table.query(KeyConditionExpression=Key('term').eq(term))
        for item in response.get('Items', []):
            doc_id = item.get('doc_id')
            tfidf = float(item.get('tfidf', '-1'))
            if doc_id is not None and tfidf > 0:
                doc_ranks[doc_id] += tfidf

    def query(self, query):
        doc_ranks = collections.defaultdict(float)
        terms = SearchRank._RE_WORDS.findall(query.lower())
        for term in terms:
            self.query_term(term, doc_ranks)
        return sorted([(rank / len(terms), doc_id) for (doc_id, rank) in doc_ranks.items()],
                      reverse=True)

    def pprint(self, results):
        for (i, (rank, doc_id)) in enumerate(results, 1):
            print("%2d. %-20s %.4f" % (i, doc_id, rank))

    def pprint_many(self, queries, top_n=0):
        for ln in queries:
            query = ln.strip().lower()
            if not query:
                continue
            print("\nQuery: %s" % query)
            results = self.query(query)
            if top_n > 0:
                results = results[:top_n]
            self.pprint(results)


def _main():
    search = SearchRank()
    queries = sys.argv[1:] if len(sys.argv) > 1 else sys.stdin
    search.pprint_many(queries, top_n=5)


if __name__ == "__main__":
    _main()
