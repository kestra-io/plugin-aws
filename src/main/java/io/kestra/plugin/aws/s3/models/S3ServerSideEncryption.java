package io.kestra.plugin.aws.s3.models;

public enum S3ServerSideEncryption {
    NONE,
    AES256,
    AWS_KMS;

    public String toHeaderValue() {
        return switch (this) {
            case AES256 -> "AES256";
            case AWS_KMS -> "aws:kms";
            default -> null;
        };
    }
}
