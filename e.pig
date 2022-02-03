-- loads file in from hdfs
accessLog = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
-- gets each users page accesses
groupByUser = GROUP accessLog BY byWho;
-- extracts needed information
-- needed because I could not figure out a way to access distinct whatPages in the next FOREACH statement
visitedPagesByUser = FOREACH groupByUser GENERATE group AS byWho, accessLog.whatPage AS whatPage;
-- finds the number distinct pages a user has visited and the total number of pages visited
distinctUsers = FOREACH visitedPagesByUser{
    distinctPages = DISTINCT whatPage;
    GENERATE byWho, COUNT(whatPage) as numberOfPagesVisited, COUNT(distinctPages) as numberOfDistinctPagesVisited;
}
-- Extracts only users that have favorites
usersWithFavorites = FILTER distinctUsers BY numberOfPagesVisited != numberOfDistinctPagesVisited;

DUMP usersWithFavorites;

