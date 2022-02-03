accessLog = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);


pagesWithVisitCounts = GROUP accessLog BY whatPage;
sumOfPageVisits = FOREACH pagesWithVisitCounts GENERATE group AS whatPage, COUNT(accessLog) as numberOfVisits;
orderedPages = ORDER sumOfPageVisits BY numberOfVisits DESC;
top8 = LIMIT orderedPages 8;



top8PageInfo = JOIN top8 BY whatPage, myPage BY id;
top8PageInfo = FOREACH top8PageInfo GENERATE id, name, nationality;



DUMP top8PageInfo;
