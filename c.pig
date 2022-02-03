-- loads file in from hdfs
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
-- groups users by their country code, so that each row corresponds to a country and its citizens that are users
groupByCountry = GROUP myPage BY countryCode;
-- counts the number of users in each country
numberOfUsersByCountry = FOREACH groupByCountry GENERATE group AS countryCode, COUNT(myPage) as numberOfUsers;
DUMP numberOfUsersByCountry;