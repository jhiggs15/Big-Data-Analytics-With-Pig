-- loads files in from hdfs
accessLog = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);

-- groups together access log by the page, so that each row is a page and all its accesses
pagesWithVisitCounts = GROUP accessLog BY whatPage;
-- counts the number of visits each page has
sumOfPageVisits = FOREACH pagesWithVisitCounts GENERATE group AS whatPage, COUNT(accessLog) as numberOfVisits;
-- order pages in descending order (ie. biggest to smallest)
orderedPages = ORDER sumOfPageVisits BY numberOfVisits DESC;
-- only extracts the top 8
top8 = LIMIT orderedPages 8;

-- joins together top8 and whatPage to extract return values of id, name and nationality
top8PageInfo = JOIN top8 BY whatPage, myPage BY id;
top8PageInfo = FOREACH top8PageInfo GENERATE id, name, nationality;

DUMP top8PageInfo;
