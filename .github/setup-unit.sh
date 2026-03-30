cat > src/test/resources/application-test.yml << EOF
kestra:
  variables:
    globals:
      awsAccessKeyId: ${AWS_ACCESS_KEY}
      awsSecretAccessKey: ${AWS_SECRET_ACCESS_KEY}
EOF
