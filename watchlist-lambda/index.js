const malScraper = require('mal-scraper');
const aws = require('aws-sdk');
const ddb = new aws.DynamoDB.DocumentClient({ region: process.env.AWS_REGION });
const cw = new aws.CloudWatch({ region: process.env.AWS_REGION });
const logger = require('logger').createLogger();
const MAX_AGE = 1000*60*60*24*30*3; // 3 months in ms

/**
 * Given a User, check if it's a newly discovered User or a stale User
 * and query and store the Anime the User has watched if new or not stale
 * 
 * Metrics and meanings:
 * CrawlableUserCount - emitted when a new or stale User is discovered
 * UncrawlableUserCount - emmitted when a User's watchlist is failed to process except when it's a duplicate
 * DuplicateUserCount - emitted when a User's data has been crawled recently
 * WatchListFailureCount - emitted when a query for a User's watchlist fails
 * PutWatchedAnimeFailureCount - emitted when a User -> watched -> Anime association fails to save
 * PutWatchedAnimeDuplicateCount - emitted when a User -> watched -> Anime association already exists
 * PutWatchedAnimeCount - emitted when a new User -> watched -> Anime association is discovered
 * 
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
  var crawlableUserCount = 0;
  var uncrawlableUserCount = 0;
  var duplicateUserCount = 0;
  var isNewOrStaleUser = true;
  var updateTime = Date.now();
  ddb.update({
    TableName: process.env.MAL_USER_TABLE_NAME,
    Key: {
      'malId': userName
    },
    UpdateExpression: 'SET lastUpdated = :t',
    ExpressionAttributeValues: {
      ':t': updateTime.toString(),
      ':s': (updateTime - MAX_AGE).toString()
    },
    ConditionExpression: 'attribute_not_exists(malId) OR lastUpdated < :s'
  }, (err, data) => {
    if (err != null) {
      if (err.code === 'ConditionalCheckFailedException') {
        duplicateUserCount += 1;
      } else {
        logger.error(`An error occurred crawling user ${userName}`);
        logger.error(err.message);
        uncrawlableUserCount += 1;
      }
      isNewOrStaleUser = false;
    }
    if (isNewOrStaleUser) {
      crawlableUserCount += 1;
    }
    cw.putMetricData({
      MetricData: [
        {
          MetricName: 'CrawlableUserCount',
          Value: crawlableUserCount,
          Unit: 'Count',
          Dimensions: [
            {
              Name: 'Region',
              Value: process.env.AWS_REGION
            }
          ]
        },
        {
          MetricName: 'UncrawlableUserCount',
          Value: uncrawlableUserCount,
          Unit: 'Count',
          Dimensions: [
            {
              Name: 'Region',
              Value: process.env.AWS_REGION
            }
          ]
        },
        {
          MetricName: 'DuplicateUserCount',
          Value: duplicateUserCount,
          Unit: 'Count',
          Dimensions: [
            {
              Name: 'Region',
              Value: process.env.AWS_REGION
            }
          ]
        }
      ],
      Namespace: process.env.NAMESPACE
    }, (err, data) => {
        if (err) {
          logger.error('An error occurred putting User crawling metrics.');
          logger.error(err.message);
        }
    });
    callback(isNewOrStaleUser);
  });
}

/**
 * Query the watchlist of a User and save the response. The response from the mal-scraper honors pagination,
 * this method will evaluate all pages.
 */
queryAndSaveWatchList = function(userName, offset) {
  watchListFailureCount = 0;
  // This promise doesn't have an onFinally ):
  // TODO emit latency metric for this call. It's understandably slow but nice to quantify.
  // It might be better to move away from serverless b/c of the slow runtime.
  malScraper.getWatchListFromUser(userName, offset, 'anime')
    .then((data) => {
      logger.info(`Successfully retreived watchlist ${offset} - ${data.length} of ${userName}`);
      saveUserWatchedRecords(userName, data);
      // Documentation says 300 is the max length of a response, local testing says 100....
      if (data.length % 100 == 0) {
        offset += data.length;
        queryAndSaveWatchList(userName, offset);
      }
      cw.putMetricData({
        MetricData: [
          {
            MetricName: 'WatchListFailureCount',
            Value: watchListFailureCount,
            Unit: 'Count',
            Dimensions: [
              {
                Name: 'Region',
                Value: process.env.AWS_REGION
              }
            ]
          }
        ],
        Namespace: process.env.NAMESPACE
      }, (err, data) => {
          if (err) {
            logger.error('An error occurred putting User crawling metrics.');
            logger.error(err.message);
          }
      });
    }).catch((err) => {
      logger.error(`Error retreiving watchlist ${offset} - ${data.length} of ${userName}`);
      logger.error(err.message);
      watchListFailureCount += 1;
      cw.putMetricData({
        MetricData: [
          {
            MetricName: 'WatchListFailureCount',
            Value: watchListFailureCount,
            Unit: 'Count',
            Dimensions: [
              {
                Name: 'Region',
                Value: process.env.AWS_REGION
              }
            ]
          }
        ],
        Namespace: process.env.NAMESPACE
      }, (err, data) => {
          if (err) {
            logger.error('An error occurred putting User crawling metrics.');
            logger.error(err.message);
          }
      });
    });
}

/**
 * Record that a User has watched a collection of Animes.
 */
saveUserWatchedRecords = function(userName, watchedAnime) {
  putWatchedAnimeFailureCount = 0;
  putWatchedAnimeDuplicateCount = 0;
  putWatchedAnimeCount = 0;
  watchedAnime.forEach(anime => {
    if (anime.status == 2) { // Magic number from MAL, means series complete
      ddb.put({
        TableName: process.env.MAL_USER_TABLE_NAME,
        Item: {
          'malId': userName + '|' + anime.animeId,
          score: anime.score
        },
        ConditionExpression: 'attribute_not_exists(malId)'
      }, (err, data) => {
        if (err != null) {
          if (err.code === 'ConditionalCheckFailedException') {
            putWatchedAnimeDuplicateCount += 1;
          } else {
            logger.error(`An error occurred saving ${userName} watched ${watchedAnime}`);
            logger.error(err.message);
            putWatchedAnimeFailureCount += 1;
          }
        } else {
          logger.info(`${userName} watched ${anime.animeId}`);
          putWatchedAnimeCount += 1;
        }
        cw.putMetricData({
          MetricData: [
            {
              MetricName: 'PutWatchedAnimeFailureCount',
              Value: putWatchedAnimeFailureCount,
              Unit: 'Count',
              Dimensions: [
                {
                  Name: 'Region',
                  Value: process.env.AWS_REGION
                }
              ]
            },
            {
              MetricName: 'PutWatchedAnimeDuplicateCount',
              Value: putWatchedAnimeDuplicateCount,
              Unit: 'Count',
              Dimensions: [
                {
                  Name: 'Region',
                  Value: process.env.AWS_REGION
                }
              ]
            },
            {
              MetricName: 'PutWatchedAnimeCount',
              Value: putWatchedAnimeCount,
              Unit: 'Count',
              Dimensions: [
                {
                  Name: 'Region',
                  Value: process.env.AWS_REGION
                }
              ]
            }
          ],
          Namespace: process.env.NAMESPACE
        }, (err, data) => {
            if (err) {
              logger.error('An error occurred putting User crawling metrics.');
              logger.error(err.message);
            }
        });
      });
    }
  });
}
