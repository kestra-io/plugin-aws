# How to use the AWS Billing plugin

The `billing` sub-plugin reads AWS Cost Explorer (cost and usage, cost forecasts, Cost Anomaly Detection) and AWS Budgets from your Kestra flows, using the same AWS credentials as the rest of your `plugin-aws` tasks.

## Cost Explorer is a paid API

AWS charges **$0.01 for each paginated Cost Explorer request** (`GetCostAndUsage`, `GetCostForecast`, `GetAnomalies`). A result that needs 100 pages costs $1.00, and a flow that runs every hour adds up quickly. To keep the bill low:

- request only the metrics, granularity and `groupBy` you need;
- keep `maxPages` low (default 10); the task stops there, logs a warning and sets `truncated: true`;
- prefer `DAILY` or `MONTHLY` granularity: `HOURLY` must be enabled on the account and is charged separately.

The AWS Budgets API used by `DescribeBudgets` and `DescribeBudget` is free.

## Authentication

All billing tasks inherit the standard `plugin-aws` authentication. Cost Explorer and Budgets are global services served from `us-east-1`, so set `region: us-east-1`. Set the credentials on each task, as in the examples (flow-level `pluginDefaults` are not accepted on Kestra 2.0). The IAM identity needs `ce:GetCostAndUsage`, `ce:GetCostForecast`, `ce:GetAnomalies` and `budgets:ViewBudget`.

```yaml
- id: costs
  type: io.kestra.plugin.aws.billing.GetCostAndUsage
  region: us-east-1
  accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
  secretKeyId: "{{ secret('AWS_SECRET_ACCESS_KEY') }}"
  # ...task properties
```

## Choosing a task

| Task | Use when |
|---|---|
| `GetCostAndUsage` | You want cost or usage for a past period, optionally grouped by service, account, tag... |
| `GetCostForecast` | You want the forecasted spend for a future period |
| `GetAnomalies` | You want the cost anomalies detected by Cost Anomaly Detection |
| `DescribeBudgets` | You want all budgets of an account with their actual and forecasted spend |
| `DescribeBudget` | You want one budget, for example to fail or alert when it is exceeded |
| `BudgetThresholdTrigger` | You want a flow to start when a budget's actual or forecasted spend crosses a percentage of its limit |
| `AnomalyTrigger` | You want a flow to start when Cost Anomaly Detection reports a new anomaly |

## Dates

`startDate` and `endDate` use the `yyyy-MM-dd` format. For `GetCostAndUsage` and `GetCostForecast`, `endDate` is **exclusive** like in Cost Explorer: the whole month of January is `startDate: 2026-01-01` and `endDate: 2026-02-01`. For `GetAnomalies`, `endDate` is **inclusive** (the last day an anomaly was observed), so January is `2026-01-01` to `2026-01-31`.

## Outputs

List tasks follow the `fetchType` convention used by `athena.Query`: `STORE` (default) writes an ION file and returns its `uri`, `FETCH` returns `rows`, `FETCH_ONE` returns `row`, and `NONE` returns only the counts. `size` is the number of rows in the output: all rows read for `FETCH`, `STORE` and `NONE`, 1 or 0 for `FETCH_ONE`. With `FETCH_ONE`, `GetCostAndUsage` and `GetAnomalies` stop requesting pages once a page returns a row, to avoid paying for pages that are discarded; `costTotals` and `totalImpact` then only cover the pages read, and `truncated` is `true` if more pages were available. `GetCostAndUsage` also returns `costTotals`, the sum of each cost metric (e.g. `costTotals.UnblendedCost`; usage metrics `UsageQuantity` and `NormalizedUsageAmount` are not totaled because their units differ), and `GetAnomalies` returns `totalImpact` in USD.

## Triggers

Both triggers are polling triggers and remember what they already reported in the namespace KV store, under a key derived from the namespace, flow and trigger IDs (set `stateKey` to choose it). They write that state only after the execution is created, so a failed poll reports again instead of losing an alert.

`BudgetThresholdTrigger` reads one budget with `DescribeBudget` (free) every `interval` (default `PT1H`) and fires when the spend selected by `thresholdType` (`ACTUAL` or `FORECASTED`) reaches `thresholdPercent` of the budget limit. It fires **once per budget period** for a given budget, threshold type and threshold: after a monthly budget fires in October, it stays quiet until November, even if the spend keeps growing or the forecast dips and crosses again. Periods are UTC calendar days, months, quarters or years, following the budget's time unit; a custom budget has a single period. Use one trigger per threshold, for example 80% `FORECASTED` and 100% `ACTUAL`. Because AWS refreshes budget spend only a few times a day, the trigger does not fire during the first `periodStartGrace` of a period (default `PT12H`, capped at a quarter of the period, so daily budgets wait 6 hours), when `DescribeBudget` may still return the previous period's spend. The IAM identity needs `budgets:ViewBudget`.

`AnomalyTrigger` calls `GetAnomalies` (paid, $0.01 per page) every `interval` (default `PT6H`; Cost Anomaly Detection runs about three times a day) for the anomalies of the last `lookback` days (default `P7D`), and starts one execution with every anomaly it has not reported before, in `trigger.anomalies`. The watermark is the ID and last observed date of each reported anomaly in the lookback window: AWS often reports an anomaly a day or more after it started, so a single "last start date" would skip those. An anomaly below `minImpactUsd` is not remembered, so it fires once its impact grows above the threshold. The IAM identity needs `ce:GetAnomalies`.
