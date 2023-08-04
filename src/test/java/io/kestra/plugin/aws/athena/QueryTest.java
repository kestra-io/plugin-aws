package io.kestra.plugin.aws.athena;

import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class QueryTest {
    @Value("${kestra.aws.access-key}")
    private String accessKey;

    @Value("${kestra.aws.secret-key}")
    private String secretKey;

    @Inject
    protected RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        var runContext = runContextFactory.of();

        var query = Query.builder()
            .id("hello")
            .type(Query.class.getName())
            .region("eu-west-3")
            .accessKeyId(accessKey)
            .secretKeyId(secretKey)
            .database("units")
            .fetchType(FetchType.FETCH)
            .outputLocation("s3://kestra-unit-test")
            .query("select * from types")
            .build();

        var output = query.run(runContext);
        assertThat(output, notNullValue());
        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), notNullValue());
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getRow(), nullValue());
        assertThat(output.getUri(), nullValue());
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("binary"), nullValue());
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("date"), is(LocalDate.parse("2008-09-15")));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("struct"), is("{name=Bob, age=38}"));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("string"), is("yeah"));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("double"), is(Double.valueOf("123.123")));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("float"), is(Float.valueOf("123.123")));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("int"), is(123));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("boolean"), is(true));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("array"), is("[1, 2, 3]"));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("decimal"), is(BigDecimal.valueOf(12312300L, 5)));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("bigint"), is(123123123123123L));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("map"), is("{bar=2, foo=1}"));
        assertThat(((Map<String, Object>) output.getRows().get(0)).get("timestamp"), is(LocalDateTime.parse("2008-09-15T03:04:05.324")));
    }
}
