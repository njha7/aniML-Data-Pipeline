const malScraper = require('mal-scraper');
const aws = require('aws-sdk');
const MAX_AGE = 1000*60*60*24*30*3; // 3 months in ms
const DDB = new aws.DynamoDB.DocumentClient({ region: process.env.AWS_REGION });

/**
 * Given a User, check if it's a newly discovered User or a stale User
 * and query and store the Anime the User has watched if new or not stale
 */ 
exports.handler = function(event, context, callback) {
  event['Records'].forEach(record => {
    const userName = record['body'];
    isCrawlable(userName, function(isNewOrStaleUser){
      if (isNewOrStaleUser) {
        queryAndSaveWatchList(userName, 0);
      }
    });
  });
};

/**
 * Check freshness of a User. Returns true if a User is fresh or new. False otherwise.
 * Best effort and fail-open. Will return false if an error occurs, logging and attempting to emit metrics.
 */
isCrawlable = function(userName, callback) {
  var updateTime = Date.now();
  DDB.update({
    TableName: process.env.MAL_USER_TABLE_NAME,
    Key: {
      'malId': userName
    },
    UpdateExpression: 'SET lastUpdated = :t',
    ExpressionAttributeValues: {
      ':t': updateTime.toString(),
      ':s': (updateTime - MAX_AGE).toString()
    },
    ConditionExpression: 'attribute_not_exists(malId) OR lastUpdated LE :s'
  }, (err, data) => {
    if (err != null) {
      // TODO metrics and logging
      callback(false);
    }
    callback(true);
  });
}

/**
 * Query the watchlist of a User and save the response. The response from the mal-scraper honors pagination,
 * this method will evaluate all pages.
 */
queryAndSaveWatchList = function(userName, offset) {
  malScraper.getWatchListFromUser(userName, offset, 'anime')
    .then((data) => {
      saveUserWatchedRecords(userName, data);
      // Documentation says 300 is the max length of a response, local testing says 100....
      if (data.length % 100 == 0) {
        offset += data.length
        queryAndSaveWatchList(userName, watchedAnimeCount)
      }
    }).catch((err) => {
      // TODO get a logger, use it and emit cloudwatch metris
      console.log(err);
    });
}

/**
 * Record that a User has watched a collection of Animes.
 */
saveUserWatchedRecords = function(userName, watchedAnime) {
  watchedAnime.forEach(anime => {
    DDB.put({
      TableName: process.env.MAL_USER_TABLE_NAME,
      Key: {
        'malId': userName + '|' + watchedAnime
      },
      Item: {
        // TODO figure out some meaningful metadata to put here.
      },
      ConditionExpression: 'attribute_not_exists(malId)'
    }, (err, data) => {
      if (err != null) {
        if (err.code === 'ConditionalCheckFailedException') {
          // TODO this is an acceptable fail state, still want to emit metrics but
          // nothing blew up which is important to note
        }
        // TODO metrics and logging
      }
      // TODO metrics and logging
    });
  });
}
