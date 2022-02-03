-- loads file in from hdfs
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
-- returns users with british nationality only
britishUsers = FILTER myPage BY nationality == 'British   ';
-- for each user only return their name and hobby
britishUsersNameAndHobby = FOREACH britishUsers GENERATE name, hobby;
DUMP britishUsersNameAndHobby;