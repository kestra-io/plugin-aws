# How to use the Amazon Bedrock plugin

The `bedrock` sub-plugin lets you invoke foundation models, run structured multi-turn conversations, and list available models directly from your Kestra workflows — using the same AWS credentials as the rest of your `plugin-aws` tasks.

## Authentication

All Bedrock tasks inherit the standard `plugin-aws` authentication. You can either set credentials directly on the task or let the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html) discover them from the environment. It is best practice to store credentials as [secrets](https://kestra.io/docs/concepts/secret).

```yaml
tasks:
  - id: chat
    type: io.kestra.plugin.aws.bedrock.Converse
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    modelId: "anthropic.claude-3-5-sonnet-20241022-v2:0"
    messages:
      - role: user
        content: "Summarize the key benefits of Apache Kafka."
```

## Choosing a task

| Task | Use when |
|---|---|
| `InvokeModel` | You need model-specific request formats (e.g. Titan, Jurassic) not covered by the Converse API |
| `Converse` | You want a unified, model-agnostic chat interface with system prompts and multi-turn history |
| `ConverseStream` | Same as `Converse` but with streaming token delivery — useful for long responses |
| `ListFoundationModels` | You need to discover available model IDs before configuring other tasks |

## Finding model IDs

Use `ListFoundationModels` to discover which models are available in your region, then copy the `modelId` value into `InvokeModel` or `Converse`.

```yaml
tasks:
  - id: list_models
    type: io.kestra.plugin.aws.bedrock.ListFoundationModels
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    byOutputModality: TEXT
```

## InvokeModel — raw payloads

`InvokeModel` is model-agnostic: you supply the full request `body` in the format the target model expects. Consult the [Bedrock API reference](https://docs.aws.amazon.com/bedrock/latest/APIReference/) for each model's schema.

```yaml
tasks:
  - id: invoke_titan
    type: io.kestra.plugin.aws.bedrock.InvokeModel
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    modelId: "amazon.titan-text-express-v1"
    body:
      inputText: "Explain event-driven architecture in one paragraph."
      textGenerationConfig:
        maxTokenCount: 256
        temperature: 0.7
```

The parsed response is available as `outputs.invoke_titan.body`.

## Converse — unified chat API

`Converse` uses Bedrock's unified Converse API, which works across all supported models without model-specific payload formatting. Messages must have `role: user` or `role: assistant`. A `system` prompt and `inferenceConfig` are optional.

```yaml
tasks:
  - id: chat
    type: io.kestra.plugin.aws.bedrock.Converse
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    modelId: "anthropic.claude-3-5-sonnet-20241022-v2:0"
    system: "You are a concise data engineering assistant."
    messages:
      - role: user
        content: "What is the difference between a data lake and a data warehouse?"
    inferenceConfig:
      maxTokens: 512
      temperature: 0.5
```

The assistant reply is available as `outputs.chat.content`.

## ConverseStream — streaming responses

`ConverseStream` has the same interface as `Converse` but streams tokens from the model and accumulates them into a single `content` output. Use it for long responses where you want the task to complete as soon as generation finishes.

```yaml
tasks:
  - id: stream_chat
    type: io.kestra.plugin.aws.bedrock.ConverseStream
    region: "{{ secret('AWS_REGION') }}"
    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
    secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
    modelId: "anthropic.claude-3-5-sonnet-20241022-v2:0"
    messages:
      - role: user
        content: "Write a detailed explanation of the CAP theorem."
    inferenceConfig:
      maxTokens: 2048
```
