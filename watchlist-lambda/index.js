const malScraper = require('mal-scraper');
const aws = require('aws-sdk');
const MAX_AGE = 1000*60*60*24*30*3; // 3 months

/**
 * Given a User, check if it's a newly discovered user or a stale user
 * and query and store the Anime the user has watched if new or not stale
 */ 
exports.handler = async function(event) {
  ddb = new aws.DynamoDB({region: process.env.REGION});
  event['Records'].forEach(record => {
    const userName = record['body']
    // staleness check
    isCrawlable(ddb, userName, function(isCrawlable){
      if (isCrawlable) {
        malScraper.getWatchListFromUser(record['body'], 0, 'anime')
        .then((data) => {
          // TODO User -> watched -> Anime storing
          // TODO if while len(list) == 100 then query with an offset of 100 until err or len(list) < 100
        }).catch((err) => {
          // TODO get a logger, use it and emit cloudwatch metris
          console.log(err);
        });
      }
    });
  });
  // TODO this return is probably useless/terminates the function early... Look into that.
  return {
    statusCode: 200,
  };
};

/**
 * Check freshness of a user. Returns true if a user is fresh or new. False otherwise.
 * Best effort and fail-open. Will return false if an error occurs, logging and attempting to emit metrics.
 */
isCrawlable = function(dynamodbClient, userName, callback) {
  dynamodbClient.UpdateItem({
    TableName: process.env.MAL_USER_TABLE_NAME,
    Key: {
      malId: {
        S: userName
      }
    },
    UpdateExpression: 'SET lastUpdated = :t',
    ExpressionAttributeValues: {
      ':t': {
        N: Date.now().toString()
      }
    },
    ConditionExpression: ''
  }, (err, data) => {
    if (err != null) {
      // TODO metrics and logging
      callback(false);
    }
    callback(true);
  });
}

