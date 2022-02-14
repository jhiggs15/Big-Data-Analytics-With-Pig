-- loads files in from hdfs

accessLog = LOAD 'input/accessLog-test.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
friends = LOAD 'input/friends-test.csv' USING PigStorage (',') AS (friendRel:int, personID:int, myFriend:int, dateOfFriendship:int, description:charArray);

-- groups together all of the friend relations in friends
groupByFriendRelations = GROUP friends BY (personID, myFriend);

-- groups together all of the page access by person, and page
groupByPageAccesses = GROUP accessLog BY (byWho, whatPage);

-- finds all of the people who has an access time further than 14 days from their first access
findBadFriends = FILTER groupByPageAccesses BY (byWho, whatPage) NOT groupByFriendRelations;


-- finds the name of people who are innactive
joinWithPages = JOIN findBadFriends BY byWho, myPage BY id;

displayName = FOREACH joinWithPages GENERATE name;


DUMP displayName;