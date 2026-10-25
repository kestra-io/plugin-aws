package io.kestra.plugin.aws.dynamodb;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DynamoDbAttributeConversionTest {
    private final GetItem task = GetItem.builder().build();

    @Test
    void shouldReadANumberAndAFalseBoolean() {
        assertThat(task.objectFrom(AttributeValue.fromN("30")), is(new BigDecimal("30")));
        assertThat(task.objectFrom(AttributeValue.fromBool(false)), is(false));
        assertThat(task.objectFrom(AttributeValue.fromBool(true)), is(true));
        assertThat(
            task.objectFrom(AttributeValue.fromNs(List.of("1", "2"))),
            is(List.of(new BigDecimal("1"), new BigDecimal("2")))
        );
        assertThat(
            task.objectFrom(AttributeValue.fromL(List.of(AttributeValue.fromN("4"), AttributeValue.fromBool(false)))),
            is(List.of(new BigDecimal("4"), false))
        );
    }

    @Test
    void shouldStoreANumberAsANumber() {
        assertThat(task.objectFrom(30).n(), is("30"));
        assertThat(task.objectFrom(false).bool(), is(false));
    }

    @Test
    void shouldStoreAStringListAsAStringSetAndAnyOtherListAsAList() {
        assertThat(task.objectFrom(List.of("a", "b")).ss(), is(List.of("a", "b")));
        assertThat(
            task.objectFrom(List.of(new BigDecimal("1"), new BigDecimal("2"))).l(),
            is(List.of(AttributeValue.fromN("1"), AttributeValue.fromN("2")))
        );
    }
}
