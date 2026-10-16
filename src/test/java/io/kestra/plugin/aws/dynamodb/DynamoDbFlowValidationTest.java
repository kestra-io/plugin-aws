package io.kestra.plugin.aws.dynamodb;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.GenericFlow;
import io.kestra.core.services.FlowService;
import io.kestra.core.tenant.TenantService;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class DynamoDbFlowValidationTest {
    @Inject
    private FlowService flowService;

    @Test
    void shouldSaveAFlowWhenScanAndQueryOmitLimit() throws Exception {
        var created = flowService.create(GenericFlow.fromYaml(TenantService.MAIN_TENANT, """
            id: dynamodb_limit_unset
            namespace: company.team

            tasks:
              - id: scan
                type: io.kestra.plugin.aws.dynamodb.Scan
                region: eu-central-1
                tableName: persons
              - id: query
                type: io.kestra.plugin.aws.dynamodb.Query
                region: eu-central-1
                tableName: persons
                keyConditionExpression: "id = :id"
                expressionAttributeValues:
                  :id: "1"
            """), true);

        assertThat(created, notNullValue());
    }
}
