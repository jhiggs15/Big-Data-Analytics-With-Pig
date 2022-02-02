myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
britishUsers = FILTER myPage BY nationality == 'British   ';
britishUsersNameAndHobby = FOREACH britishUsers GENERATE name, hobby;
DUMP britishUsersNameAndHobby;