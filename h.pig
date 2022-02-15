-- loads files in from hdfs

myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
friends = LOAD 'input/friends-test.csv' USING PigStorage (',') AS (friendRel:int, personID:int, myFriend:int, dateOfFriendship:int, description:charArray);


-- groups together access log by the page, so that each row is a page and all its accesses
--friendsGrouped = FOREACH friends GENERATE friendRel;
--sumOfPeople = FOREACH friendsGrouped GENERATE COUNT(friends) AS count;

friendsGroup = GROUP friends ALL;
friendCount = FOREACH friendsGroup GENERATE COUNT(friends) AS hello;
-- the number of users / number of friends

---- counts the number of friends each person has
--sumOfFriends = FOREACH groupByFriends GENERATE group AS personID, COUNT(friends) as numberOfFriends;
--
---- finds the average amount of friends across the friends file
--averageOfFriends = FOREACH numberOfFriends GENERATE (friends.personID), AVG(numberOfFriends);
--
---- filters out the people with a higher frined count than the average
--findFamousPeople = FILTER sumOfFriends BY (numberOfFriends > averageOfFriends);
--
---- finds the name of people who are famous
--joinWithPages = JOIN findFamousPeople BY personID, myPage BY id;


DUMP friendCount.$0;