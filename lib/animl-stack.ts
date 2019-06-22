import cdk = require('@aws-cdk/cdk');
import ddb = require('@aws-cdk/aws-dynamodb');
import lambda = require('@aws-cdk/aws-lambda');
import sqs = require('@aws-cdk/aws-sqs');
import s3 = require('@aws-cdk/aws-s3');
import { SqsEventSource, DynamoEventSource } from '@aws-cdk/aws-lambda-event-sources';
import { Metric } from '@aws-cdk/aws-cloudwatch';

const CLOUDWATCH_NAMESPACE = 'AniML'

export class AnimlStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // TODO ECS resources to host the crawler code.
    // https://docs.aws.amazon.com/cdk/latest/guide/ecs_example.html

    // Queue to recieve users to query watchlists for
    const malUserQueue = new sqs.Queue(this, 'malUserQueue');

    // Dynamo table for storing users and show's they've watched. We assume that a watchlist is append only
    // (this is not a rigidly enforced assumption anywhere but a fair one given that you can't "unwatch something")
    // using conditional writes and a stream, only new users and user -> watched associations will be captured.
    const malUserTable = new ddb.Table(this, 'malUserTable', {
      tableName: 'malUser',
      partitionKey: {
        name: 'malId',
        type: ddb.AttributeType.String
      },
      billingMode: ddb.BillingMode.PayPerRequest,
      streamSpecification: ddb.StreamViewType.NewImage,
    });

    // Lambda function to capture user -> watched associations 
    const malUserQueueConsumer = new lambda.Function(this, 'malUserQueueConsumer', {
      runtime: lambda.Runtime.NodeJS810,
      code: lambda.Code.asset('watchlist-lambda'),
      handler: 'index.handler',
      logRetentionDays: 14,
      timeout: 900,
      memorySize: 256,
      environment: {
        MAL_USER_TABLE_NAME: malUserTable.tableName,
        NAMESPACE: CLOUDWATCH_NAMESPACE
      }
    });
    new SqsEventSource(malUserQueue).bind(malUserQueueConsumer)

    // S3 Bucket for storing User -> watched -> Anime associations in protobuf recordIO format
    const malWatchedBucket = new s3.Bucket(this, 'malWatchedBucket');

    // Lambda function for taking new User -> watched -> Anime associations and storing them in S3 for SageMaker
    const malUserTableStreamConsumer = new lambda.Function(this, 'malUserTableStreamConsumer', {
      runtime: lambda.Runtime.NodeJS810,
      code: lambda.Code.asset('watched-lambda'),
      handler: 'index.handler',
      logRetentionDays: 14,
      environment: {
        WATCHLIST_BUCKET: malWatchedBucket.bucketName,
        NAMESPACE: CLOUDWATCH_NAMESPACE
      }
    });
    new DynamoEventSource(malUserTable, {
      startingPosition: lambda.StartingPosition.TrimHorizon
    }).bind(malUserTableStreamConsumer)

    // IAM permissions delegation
    // Allow queue consumer to read messages and r/w to ddb
    malUserQueue.grantConsumeMessages(malUserQueueConsumer);
    malUserTable.grantFullAccess(malUserQueueConsumer);
    // Allow the function to put metrics
    Metric.grantPutMetricData(malUserQueueConsumer);
    // Allow stream consumer to read ddb stream and write to s3
    malUserTable.grantStreamRead(malUserTableStreamConsumer);
    malWatchedBucket.grantPut(malUserTableStreamConsumer);
  }
}
