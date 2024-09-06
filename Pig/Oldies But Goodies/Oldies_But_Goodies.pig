--Creating a relation 'ratings' 
--Loading the Ratings data from your HDFS
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

--Creating a relation 'metadata'
--Loading the Movies data from the HDFS. It is '|' (pipe) separated
metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

--Sort them by Release Date
--Use FOREACH GENERATE to generate a new relation from an existing relation 
--Goes through each row and generates a new relation with the following columns
--Convert the Character array to a Unix Timestamp
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

--Grouping them by their MovieID (The Reduce Operation)
ratingsByMovie = GROUP ratings BY movieID;

--Have a look at the structure of 
--DESCRIBE ratingsByMovie 
--Output: ratingsByMovie: {group: int, ratings:{(userID: int, movieID: int, rating: int, ratingTime: int)}}
--DUMP ratingsByMovie 
--Output: (1, {(807,1,4,89898889898), (554,1,3,35353453)....})

--Get the Average Rating for Each Movie
avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

--Filtering movies with Average rating > 4 
fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

--Joining fiveStarMovies & nameLookup relations using movieID
fiveStarsWithData = JOIN fiveStarMovies By movieID, nameLookup BY movieID;

--Order them by Release Time
--"::" - because of the join (Syntax)
oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

--Output
DUMP oldestFiveStarMovies;
