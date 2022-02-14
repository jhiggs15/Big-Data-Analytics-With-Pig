-- loads files in from hdfs

accessLog = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);


-- groups together access log by the page, so that each row is a page and all its accesses
groupByAccesses = GROUP accessLog BY byWho;

-- finds the minimum access time (first access) of each person
firstAccessTime = FOREACH groupByAccesses generate (accessLog.byWho), MIN(accessLog.accessTime);

-- finds all of the people who has an access time further than 14 days from their first access
findLostInterest = FILTER groupByAccesses BY (accessLog.accessTime > (firstAccessTime)+ 1400));


-- finds the name of people who are innactive
joinWithPages = JOIN findLostInterest BY byWho, myPage BY id;


DUMP joinWithPages;