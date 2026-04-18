# Kestra AWS Plugin

## What

- Provides plugin components under `io.kestra.plugin.aws`.
- Includes classes such as `ConnectionUtils`, `Consume`, `PutRecords`, `Trigger`.

## Why

- This plugin integrates Kestra with Athena.
- It provides tasks that run Amazon Athena SQL queries against data in S3 or federated sources, writing results to an S3 output location and optionally fetching rows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `aws`

### Key Plugin Classes

- `io.kestra.plugin.aws.athena.Query`
- `io.kestra.plugin.aws.auth.EksToken`
- `io.kestra.plugin.aws.cli.AwsCLI`
- `io.kestra.plugin.aws.cloudformation.Create`
- `io.kestra.plugin.aws.cloudformation.Delete`
- `io.kestra.plugin.aws.cloudwatch.Push`
- `io.kestra.plugin.aws.cloudwatch.Query`
- `io.kestra.plugin.aws.cloudwatch.Trigger`
- `io.kestra.plugin.aws.dynamodb.DeleteItem`
- `io.kestra.plugin.aws.dynamodb.GetItem`
- `io.kestra.plugin.aws.dynamodb.PutItem`
- `io.kestra.plugin.aws.dynamodb.Query`
- `io.kestra.plugin.aws.dynamodb.Scan`
- `io.kestra.plugin.aws.ecr.GetAuthToken`
- `io.kestra.plugin.aws.emr.CreateClusterAndSubmitSteps`
- `io.kestra.plugin.aws.emr.CreateServerlessApplicationAndStartJob`
- `io.kestra.plugin.aws.emr.DeleteCluster`
- `io.kestra.plugin.aws.emr.DeleteServerlessApplication`
- `io.kestra.plugin.aws.emr.StartServerlessJobRun`
- `io.kestra.plugin.aws.emr.SubmitSteps`
- `io.kestra.plugin.aws.eventbridge.PutEvents`
- `io.kestra.plugin.aws.glue.GetJobRun`
- `io.kestra.plugin.aws.glue.StartJobRun`
- `io.kestra.plugin.aws.glue.StopJobRun`
- `io.kestra.plugin.aws.kinesis.Consume`
- `io.kestra.plugin.aws.kinesis.PutRecords`
- `io.kestra.plugin.aws.kinesis.RealtimeTrigger`
- `io.kestra.plugin.aws.kinesis.Trigger`
- `io.kestra.plugin.aws.lambda.Invoke`
- `io.kestra.plugin.aws.s3.Copy`
- `io.kestra.plugin.aws.s3.CreateBucket`
- `io.kestra.plugin.aws.s3.Delete`
- `io.kestra.plugin.aws.s3.DeleteList`
- `io.kestra.plugin.aws.s3.Download`
- `io.kestra.plugin.aws.s3.Downloads`
- `io.kestra.plugin.aws.s3.List`
- `io.kestra.plugin.aws.s3.Trigger`
- `io.kestra.plugin.aws.s3.Upload`
- `io.kestra.plugin.aws.sns.Publish`
- `io.kestra.plugin.aws.sqs.Consume`
- `io.kestra.plugin.aws.sqs.Publish`
- `io.kestra.plugin.aws.sqs.RealtimeTrigger`
- `io.kestra.plugin.aws.sqs.Trigger`

### Project Structure

```
plugin-aws/
├── src/main/java/io/kestra/plugin/aws/sqs/
├── src/test/java/io/kestra/plugin/aws/sqs/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
