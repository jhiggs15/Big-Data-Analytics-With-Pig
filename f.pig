-- loads files in from hdfs

accessLog = LOAD 'input/accessLog.csv' USING PigStorage (',') AS (accessID:int, byWho:int, whatPage:int, typeOfAccess:charArray, accessTime:int);
myPage = LOAD 'input/myPage.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
friends = LOAD 'input/friends.csv' USING PigStorage (',') AS (friendRel:int, personID:int, myFriend:int, dateOfFriendship:int, description:charArray);

-- groups together all of the friend relations in friends
groupByFriendRelations = GROUP friends BY (personID, myFriend);
--groupByFriendRelations = FOREACH friends GENERATE group as person;
--
--dump groupByFriendRelations;

-- groups together all of the page access by person, and page
groupByPageAccesses = GROUP accessLog BY (byWho, whatPage);

-- finds all of the people who have added a friend, but never accessed their page
--findBadFriends = FILTER groupByPageAccesses BY NOT ((byWho, whatPage) MATCHES (friends.personID, myFriend));


-- finds the names of bad friends
joinWithPages = JOIN groupByFriendRelations BY $0 RIGHT OUTER, groupByPageAccesses BY $0;



uniquePages = FILTER joinWithPages BY $0 is null;



DUMP uniquePages;
