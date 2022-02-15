-- loads files in from hdfs

accessLog = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);


-- groups together access log by the page, so that each row is a page and all its accesses
groupByAccesses = GROUP accessLog BY byWho;

-- finds the minimum access time (first access) of each person
firstAccessTime = FOREACH groupByAccesses GENERATE group AS byWho, MAX(accessLog.accessTime) AS maxAccessTime;

-- finds all of the people who has an access time further than 5 days from their first access
findLostInterest = FILTER firstAccessTime BY ((1000000 - maxAccessTime) >= 432000);

DUMP findLostInterest;