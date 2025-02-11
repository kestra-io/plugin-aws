### Authentication

All tasks must be authenticated for the AWS Platform.

You can either set up the credentials in the task or let the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) search for credentials.

To set up the credentials in the task, you can use:
- The `accessKeyId` and `secretKeyId` properties for using the static credential provider,
- The `sessionToken` property in conjunction with `accessKeyId` and `secretKeyId` for temporal credentials,
- Or the `stsRoleArn`,  `stsRoleExternalII` and `stsRoleSessionName` properties for STS assume role credentials.

When defining credentials in the task, this is a best practice to use [secrets](https://kestra.io/docs/concepts/secret).
Check the blueprints such as [this one](https://kestra.io/blueprints/api-to-s3),
showing how you can reference secrets in your AWS tasks.

The [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) is an AWS credentials provider chain
that looks for credentials in this order:
- **Java System Properties** - Java system properties with variables `aws.accessKeyId` and `aws.secretAccessKey`.
- **Environment Variables** - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- Web Identity Token credentials from system properties or environment variables.
- Credential profiles file in the default location (`~/.aws/credentials`) shared by all AWS SDKs and the AWS CLI.
- Credentials provided by the Amazon EC2 container service if the `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` environment variable is set and the security manager has permission to access the variable.
- Instance profile credentials provided by the Amazon EC2 metadata service.
