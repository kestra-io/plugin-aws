package io.kestra.plugin.aws.emr.models;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.kestra.plugin.aws.emr.models.StepConfig.commandToAwsArguments;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class StepConfigTest {
    @Test
    void createArgumentsFromCommandList() {
        var commands = List.of("spark-submit s3://mybucket/health_violations.py --data_source s3://mybucket/food_establishment_data.csv --output_uri s3://mybucket/test-emr-output");

        var expected = List.of(
            "spark-submit",
            "s3://mybucket/health_violations.py",
            "--data_source",
            "s3://mybucket/food_establishment_data.csv",
            "--output_uri",
            "s3://mybucket/test-emr-output"
        );

        assertThat(commandToAwsArguments(commands), is(expected));
    }
}