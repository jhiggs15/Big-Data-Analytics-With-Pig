myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
groupByCountry = GROUP myPage BY countryCode;
numberOfUsersByCountry = FOREACH groupByCountry GENERATE group AS countryCode, COUNT(myPage) as numberOfUsers;
DUMP numberOfUsersByCountry;