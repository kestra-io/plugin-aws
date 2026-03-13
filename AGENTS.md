# Kestra AWS Plugin

## What

AWS plugin for Kestra Exposes 43 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with AWS, allowing orchestration of AWS-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
