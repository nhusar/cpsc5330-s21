
CREATE EXTERNAL TABLE IF NOT EXISTS doc_size (
    doc_id string,
    term_count int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/textcorpora-doc-size';

CREATE EXTERNAL TABLE IF NOT EXISTS doc_term_count (
    doc_id string,
    term string,
    term_count int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/textcorpora-doc-term-count';

CREATE EXTERNAL TABLE IF NOT EXISTS term_docs (
    term string,
    doc_count int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/textcorpora-df';

DROP TABLE IF EXISTS tfidf;
CREATE TABLE tfidf
ROW FORMAT DELIMITED FIELDS TERMINATED by '\t'
STORED AS TEXTFILE LOCATION '/data-output/tfidf'
AS SELECT
    doc_term_count.doc_id,
    doc_term_count.term,
    1000000 * (doc_term_count.term_count / doc_size.term_count) / term_docs.doc_count AS tfidf
FROM doc_term_count
JOIN doc_size ON (doc_size.doc_id = doc_term_count.doc_id)
JOIN term_docs ON (term_docs.term = doc_term_count.term)
ORDER BY tfidf DESC;