# How to use the Amazon MSK plugin

The `msk` sub-plugin lets you manage Amazon MSK (Managed Streaming for Apache Kafka) cluster lifecycle directly from your Kestra workflows. Use it to provision, inspect, and tear down MSK clusters, and to retrieve bootstrap broker strings that can be passed directly to `plugin-kafka` tasks.

## Authentication

All MSK tasks inherit the standard `plugin-aws` authentication. Store credentials as [secrets](https://kestra.io/docs/concepts/secret).

```yaml
tasks:
  - id: get_brokers
    type: io.kestra.plugin.aws.msk.GetBootstrapBrokers
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"
```

## Choosing a task

| Task | Use when |
|---|---|
| `GetBootstrapBrokers` | You need broker connection strings to configure `plugin-kafka` tasks |
| `ListClusters` | You need to discover available clusters in the region |
| `DescribeCluster` | You want to check a cluster's state, Kafka version, or broker count |
| `CreateCluster` | You want to provision a new MSK cluster |
| `DeleteCluster` | You want to decommission a cluster |
| `Trigger` | You want to fire a flow when a cluster reaches a target state |

## Getting bootstrap brokers

`GetBootstrapBrokers` is the primary integration point with `plugin-kafka`. It returns all available connection strings for the cluster — use `bootstrapBrokerString` for plaintext, `bootstrapBrokerStringTls` for TLS, and `bootstrapBrokerStringSaslIam` for IAM authentication.

```yaml
tasks:
  - id: get_brokers
    type: io.kestra.plugin.aws.msk.GetBootstrapBrokers
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"
```

Pass the broker string to downstream Kafka tasks:

```yaml
  - id: produce_message
    type: io.kestra.plugin.kafka.Produce
    properties:
      bootstrap.servers: "{{ outputs.get_brokers.bootstrapBrokerStringTls }}"
```

## Creating a cluster

`CreateCluster` is asynchronous — the cluster enters `CREATING` state immediately and becomes `ACTIVE` after a few minutes. Use `DescribeCluster` or the `Trigger` task to wait for `ACTIVE` before connecting producers or consumers.

```yaml
tasks:
  - id: create_cluster
    type: io.kestra.plugin.aws.msk.CreateCluster
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    clusterName: "my-kestra-msk"
    kafkaVersion: "3.5.1"
    numberOfBrokerNodes: 3
    instanceType: "kafka.m5.large"
    clientSubnets:
      - "{{ secret('SUBNET_A') }}"
      - "{{ secret('SUBNET_B') }}"
      - "{{ secret('SUBNET_C') }}"
    securityGroups:
      - "{{ secret('SECURITY_GROUP_ID') }}"
```

## Triggering flows on cluster state change

Use the `Trigger` task to fire a flow when a cluster reaches a specific state:

```yaml
triggers:
  - id: cluster_ready
    type: io.kestra.plugin.aws.msk.Trigger
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    clusterArn: "{{ secret('MSK_CLUSTER_ARN') }}"
    targetState: ACTIVE
    interval: PT1M
```

Valid `targetState` values: `ACTIVE`, `CREATING`, `DELETING`, `FAILED`, `HEALING`, `MAINTENANCE`, `REBOOTING_BROKER`, `UPDATING`.

The trigger exposes `{{ trigger.clusterArn }}` and `{{ trigger.clusterState }}` to downstream tasks.

## Ephemeral cluster pattern

A common cost-optimization pattern is to spin up a cluster, run your pipeline, then tear it down:

```yaml
tasks:
  - id: create_cluster
    type: io.kestra.plugin.aws.msk.CreateCluster
    # ... cluster config

  - id: get_brokers
    type: io.kestra.plugin.aws.msk.GetBootstrapBrokers
    clusterArn: "{{ outputs.create_cluster.clusterArn }}"
    # ... credentials

  - id: run_pipeline
    type: io.kestra.plugin.kafka.Produce
    properties:
      bootstrap.servers: "{{ outputs.get_brokers.bootstrapBrokerStringTls }}"
    # ...

  - id: delete_cluster
    type: io.kestra.plugin.aws.msk.DeleteCluster
    clusterArn: "{{ outputs.create_cluster.clusterArn }}"
    # ... credentials
```
