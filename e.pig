myPage = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
groupByUser = GROUP myPage BY byWho;
visitedPagesByUser = FOREACH groupByUser GENERATE group AS byWho, myPage.whatPage AS whatPage;

distinctUsers = FOREACH visitedPagesByUser{
    distinctPages = DISTINCT whatPage;
    GENERATE byWho, COUNT(whatPage) as numberOfPagesVisited, COUNT(distinctPages) as numberOfDistinctPagesVisited;
}

usersWithFavorites = FILTER distinctUsers BY numberOfPagesVisited != numberOfDistinctPagesVisited;

DUMP usersWithFavorites;

