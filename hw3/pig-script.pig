
-- use single file for development
ad = LOAD 's3://bigdatateaching/bigrams/googlebooks-eng-us-all-2gram-20120701-iz' AS (bigram:chararray, year:int, count:int, books:int);
-- use wildcard to select all "i" files
-- ad = LOAD 's3://bigdatateaching/bigrams/googlebooks-eng-us-all-2gram-20120701-i?' AS (bigram:chararray, year:int, count:int, books:int);

gb = GROUP ad BY bigram;
agb = FOREACH gb GENERATE 
	group AS bigram, 
	(double)SUM(ad.count) AS tot_ct,
	(double)SUM(ad.books) AS tot_books,
	(double)SUM(ad.count)/(double)SUM(ad.books) AS book_average,
	MIN(ad.year) as first_year,
	MAX(ad.year) as last_year,
	COUNT(ad.year) AS years;

-- optional: store into hdfs in case you want to run more analytics on the aggregated dataset - only need to create once
interm = STORE agb INTO 'i-bigrams';

sm = LOAD 'i-bigrams' AS (bigram:chararray, tot_ct:int, tot_books:int, book_average:double, first_yr:int, last_yr:int, ct:int);

X = FILTER sm BY first_yr == 1950 AND ct == 60;
Y = ORDER X BY book_average DESC, bigram;
Z = LIMIT Y 10;
STORE Z INTO 'pig-results' USING PigStorage(',');




