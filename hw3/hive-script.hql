create external table hive_bigrams (
bigram string, 
tot_ct int,
tot_books int,
book_average double,
first_yr int,
last_yr int, 
ct int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
LOCATION  '/user/hadoop/hive_bigrams/';

INSERT OVERWRITE DIRECTORY '/user/hadoop/hive_result/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
select * FROM hive_bigrams
where first_yr == 1950 AND ct == 60
ORDER BY book_average DESC, bigram
