# How to use the AWS plugin

Authenticate with task-level credentials or let the default credentials provider chain discover them from the environment.

## Authentication

All tasks must be authenticated for the AWS Platform.

You can either set up the credentials in the task or let the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) search for credentials.

To set up the credentials in the task, you can use:
- The `accessKeyId` and `secretKeyId` properties for using the static credential provider
- The `sessionToken` property in conjunction with `accessKeyId` and `secretKeyId` for temporal credentials
- Or the `stsRoleArn`, `stsRoleExternalId` and `stsRoleSessionName` properties for STS assume role credentials

When defining credentials in the task, it is best practice to use [secrets](https://kestra.io/docs/concepts/secret).

The [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) looks for credentials in this order:
- **Java System Properties** - `aws.accessKeyId` and `aws.secretAccessKey`
- **Environment Variables** - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Web Identity Token credentials from system properties or environment variables
- Credential profiles file at the default location (`~/.aws/credentials`)
- Amazon EC2 container service credentials via `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`
- Instance profile credentials from the Amazon EC2 metadata service

## Common properties

Set `region` on each task or globally using [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) to avoid repeating it. Most tasks also accept `endpointOverride` for connecting to custom or locally hosted endpoints such as LocalStack.

## Tasks

Tasks span the most commonly used AWS services. The `s3` package covers uploads, downloads, copies, deletions, and file-arrival triggers. For messaging and streaming, `sqs` and `sns` handle queue consumption and publishing while `kinesis` covers high-throughput record streaming; both SQS and Kinesis offer a polling `Trigger` (one execution per batch) and a `RealtimeTrigger` (one execution per record) — use `Trigger` for controlled throughput and `RealtimeTrigger` for low-latency processing.

For data and compute, `athena` queries S3 data with SQL, `glue` manages ETL jobs, `emr` runs Spark and Hadoop workloads, and `lambda` invokes serverless functions. `dynamodb` covers key-value reads and writes, `cloudwatch` handles metrics and log querying, and `eventbridge` publishes events to an event bus. Use `cli.AwsCLI` for any operation not covered by a dedicated task.
