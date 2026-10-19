# How to use the AWS Billing plugin

The `billing` sub-plugin reads AWS Cost Explorer (cost and usage, cost forecasts, Cost Anomaly Detection) and AWS Budgets from your Kestra flows, using the same AWS credentials as the rest of your `plugin-aws` tasks.

## Cost Explorer is a paid API

AWS charges **$0.01 for each paginated Cost Explorer request** (`GetCostAndUsage`, `GetCostForecast`, `GetAnomalies`). A result that needs 100 pages costs $1.00, and a flow that runs every hour adds up quickly. To keep the bill low:

- request only the metrics, granularity and `groupBy` you need;
- keep `maxPages` low (default 10); the task stops there, logs a warning and sets `truncated: true`;
- prefer `DAILY` or `MONTHLY` granularity: `HOURLY` must be enabled on the account and is charged separately.

The AWS Budgets API used by `DescribeBudgets` and `DescribeBudget` is free.

## Authentication

All billing tasks inherit the standard `plugin-aws` authentication. Cost Explorer and Budgets are global services served from `us-east-1`, so set `region: us-east-1`. The IAM identity needs `ce:GetCostAndUsage`, `ce:GetCostForecast`, `ce:GetAnomalies` and `budgets:ViewBudget`.

```yaml
pluginDefaults:
  - type: io.kestra.plugin.aws
    values:
      region: us-east-1
      accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
      secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
```

## Choosing a task

| Task | Use when |
|---|---|
| `GetCostAndUsage` | You want cost or usage for a past period, optionally grouped by service, account, tag... |
| `GetCostForecast` | You want the forecasted spend for a future period |
| `GetAnomalies` | You want the cost anomalies detected by Cost Anomaly Detection |
| `DescribeBudgets` | You want all budgets of an account with their actual and forecasted spend |
| `DescribeBudget` | You want one budget, for example to fail or alert when it is exceeded |

## Dates

`startDate` and `endDate` use the `yyyy-MM-dd` format. For `GetCostAndUsage` and `GetCostForecast`, `endDate` is **exclusive** like in Cost Explorer: the whole month of January is `startDate: 2026-01-01` and `endDate: 2026-02-01`. For `GetAnomalies`, `endDate` is **inclusive** (the last day an anomaly was observed), so January is `2026-01-01` to `2026-01-31`.

## Outputs

List tasks follow the `fetchType` convention used by `athena.Query`: `STORE` (default) writes an ION file and returns its `uri`, `FETCH` returns `rows`, `FETCH_ONE` returns `row`, and `NONE` returns only the counts. `size` is always the number of rows. With `FETCH_ONE`, `GetCostAndUsage` and `GetAnomalies` stop requesting pages once a page returns a row, to avoid paying for pages that are discarded; `costTotals` and `totalImpact` then only cover the pages read, and `truncated` is `true` if more pages were available. `GetCostAndUsage` also returns `costTotals`, the sum of each cost metric (e.g. `costTotals.UnblendedCost`; usage metrics `UsageQuantity` and `NormalizedUsageAmount` are not totaled because their units differ), and `GetAnomalies` returns `totalImpact` in USD.
