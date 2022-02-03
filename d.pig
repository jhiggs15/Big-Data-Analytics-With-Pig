-- loads files in from hdfs
myPage = LOAD 'input/myPage-test.csv' USING PigStorage (',') AS (id:int, name:charArray, nationality:charArray, countryCode:int, hobby:charArray);
friends = LOAD 'input/friends-test.csv' USING PigStorage (',') AS (friendRel:int, personID:int, myFriend:int, dateOfFriendship:int, description:charArray);
-- extracts ids for each user, does this so that each user can have a happiness of 0
allUsers = FOREACH myPage GENERATE id;
-- groups each occurence based on the number of occurences someone listed a user as a friend
friendGroupingByRecipient = GROUP friends BY myFriend;
-- count the number of times someone listed a user as a friend (happiness score)
happinessByUser = FOREACH friendGroupingByRecipient GENERATE group as user, COUNT(friends) as happiness;
-- join together allUsers and happinessByUser, this results in nulls for users that have a happiness score of 0,
--   while keeping the happiness score of users that have happiness scores greater than 0
allUsersHappinessJoined = JOIN allUsers BY id LEFT, happinessByUser BY user;
-- computes happiness scores by replacing null with 0, or by taking the previous calculated score
allUsersHappiness = FOREACH allUsersHappinessJoined GENERATE id, (happiness is null ? 0 : happiness);

DUMP allUsersHappiness;