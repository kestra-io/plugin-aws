### Authentication

All tasks must be authenticated for the AWS Platform. You can either set up the credentials using the `DefaultCredentialsProvider` or explicitly set credentials using [secrets](https://kestra.io/docs/concepts/secret) or environment variables.

The `DefaultCredentialsProvider` is an AWS credentials provider chain that looks for credentials in this order:
- **Java System Properties** - Java system properties with variables `aws.accessKeyId` and `aws.secretAccessKey`. You can explicitly define those values for a given AWS task and reference [secrets](https://kestra.io/docs/concepts/secret) when using them. Check the blueprints such as [this one](https://kestra.io/blueprints/118-extract-data-from-an-api-and-load-it-to-s3-on-schedule-(every-friday-afternoon)), showing how you can reference secrets in your AWS tasks.
- **Environment Variables** - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- Web Identity Token credentials from system properties or environment variables.
- Credential profiles file in the default location (`~/.aws/credentials`) shared by all AWS SDKs and the AWS CLI.
- Credentials provided by the Amazon EC2 container service if the `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` environment variable is set and the security manager has permission to access the variable.
- Instance profile credentials provided by the Amazon EC2 metadata service.
