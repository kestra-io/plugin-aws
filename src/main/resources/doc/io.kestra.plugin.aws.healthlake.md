# How to use the Amazon HealthLake plugin

The `healthlake` sub-plugin lets you manage FHIR R4 data stores and orchestrate bulk import and export jobs directly from your Kestra workflows, using the same AWS credentials as the rest of your `plugin-aws` tasks.

## Authentication

All HealthLake tasks inherit the standard `plugin-aws` authentication. You can set credentials directly on the task or let the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) discover them from the environment. Store credentials as [secrets](https://kestra.io/docs/concepts/secret).

```yaml
tasks:
  - id: start_import
    type: io.kestra.plugin.aws.healthlake.StartImportJob
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
    inputS3Uri: "s3://my-bucket/fhir/input/"
    outputS3Uri: "s3://my-bucket/fhir/output/"
    dataAccessRoleArn: "{{ secret('HEALTHLAKE_ROLE_ARN') }}"
```

## Choosing a task

| Task | Use when |
|---|---|
| `CreateDatastore` | You need to provision a new FHIR R4 data store |
| `DescribeDatastore` | You want to check the status or metadata of a data store |
| `ListDatastores` | You need to discover available data stores in the region |
| `DeleteDatastore` | You want to remove a data store |
| `StartImportJob` | You want to bulk-import FHIR data from S3 |
| `DescribeImportJob` | You want to poll the status of a running import job |
| `StartExportJob` | You want to bulk-export FHIR data to S3 |
| `DescribeExportJob` | You want to poll the status of a running export job |
| `Trigger` | You want to fire a flow when an import or export job reaches a terminal state |

## Managing data stores

Use `CreateDatastore` to provision a new FHIR R4 data store, then `DescribeDatastore` to wait until it reaches `ACTIVE` status.

```yaml
tasks:
  - id: create_store
    type: io.kestra.plugin.aws.healthlake.CreateDatastore
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    datastoreName: "my-fhir-datastore"
```

Use `ListDatastores` to find existing data store IDs:

```yaml
tasks:
  - id: list_stores
    type: io.kestra.plugin.aws.healthlake.ListDatastores
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    filterStatus: ACTIVE
```

## Running a bulk import

`StartImportJob` submits a bulk FHIR import job from S3 and returns the `jobId`. The `dataAccessRoleArn` must have `s3:GetObject` permission on the input bucket and `s3:PutObject` on the output bucket.

```yaml
tasks:
  - id: start_import
    type: io.kestra.plugin.aws.healthlake.StartImportJob
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
    inputS3Uri: "s3://my-bucket/fhir/input/"
    outputS3Uri: "s3://my-bucket/fhir/output/"
    dataAccessRoleArn: "{{ secret('HEALTHLAKE_ROLE_ARN') }}"
```

Use `DescribeImportJob` to poll for completion:

```yaml
  - id: check_import
    type: io.kestra.plugin.aws.healthlake.DescribeImportJob
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
    jobId: "{{ outputs.start_import.jobId }}"
```

## Running a bulk export

`StartExportJob` exports all FHIR resources from a data store to S3. The `dataAccessRoleArn` must have `s3:PutObject` on the output bucket.

```yaml
tasks:
  - id: start_export
    type: io.kestra.plugin.aws.healthlake.StartExportJob
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
    outputS3Uri: "s3://my-bucket/fhir/export/"
    dataAccessRoleArn: "{{ secret('HEALTHLAKE_ROLE_ARN') }}"
```

## Triggering flows on job completion

Use the `Trigger` task to automatically fire a flow when a HealthLake job reaches a terminal state (`COMPLETED`, `COMPLETED_WITH_ERRORS`, or `FAILED`):

```yaml
triggers:
  - id: import_done
    type: io.kestra.plugin.aws.healthlake.Trigger
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    datastoreId: "{{ secret('HEALTHLAKE_DATASTORE_ID') }}"
    jobType: IMPORT
    interval: PT2M
```

The trigger exposes `{{ trigger.jobId }}` and `{{ trigger.jobStatus }}` to downstream tasks.
