--Using HBase with Pig to import large scales of data 

users = LOAD '/user/maria_dev/ml-100k/u.user' 
-- Pipe delimiter
USING PigStorage('|') 
AS (userID:int, age:int, gender:chararray, occupation:chararray, zip:int);

STORE users INTO 'hbase://users' 
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage (
'userinfo:age,userinfo:gender,userinfo:occupation,userinfo:zip');

--Shell commands
--pig hbase.pig
--scan 'users'
--disable 'users'
--drop 'users'
--list
--exit
