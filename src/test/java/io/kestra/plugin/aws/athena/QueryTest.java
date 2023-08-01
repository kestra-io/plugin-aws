package io.kestra.plugin.aws.athena;

import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Disabled("Not available via localstack")
@MicronautTest
class QueryTest {
    private static final String ACCESS_KEY = "";
    private static final String SECRET_KEY = "";

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .id("hello")
            .type(Query.class.getName())
            .region("eu-north-1")
            .accessKeyId(ACCESS_KEY)
            .secretKeyId(SECRET_KEY)
            .database("my_database")
            .fetchType(FetchType.FETCH)
            .outputLocation("s3://kestra-athena-output")
            .query("select * from cloudfront_logs limit 10")
            .build();

        var output = query.run(runContext);
        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(10L));
        assertThat(output.getRows(), notNullValue());
        assertThat(output.getRows().size(), is(10));
        assertThat(output.getRow(), nullValue());
        assertThat(output.getUri(), nullValue());
    }
}