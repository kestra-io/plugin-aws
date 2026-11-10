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
        assertThat(task.objectFrom(List.of()).type(), is(AttributeValue.Type.L));
        var duplicates = task.objectFrom(List.of("a", "a"));
        assertThat(duplicates.type(), is(AttributeValue.Type.L));
        assertThat(duplicates.l(), is(List.of(AttributeValue.fromS("a"), AttributeValue.fromS("a"))));
    }

    @Test
    void shouldReadBinaryWithoutLosingBytes() {
        byte[] original = {0, 1, 2};

        var attribute = AttributeValue.fromB(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(original)
        );

        var result = task.objectFrom(attribute);

        assertThat(result, is("AAEC"));

        org.junit.jupiter.api.Assertions.assertArrayEquals(
            original,
            java.util.Base64.getDecoder().decode((String) result)
        );
    }


    @Test
    void shouldReadBinarySetWithoutLosingMembers() {
        var attribute = AttributeValue.fromBs(List.of(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {(byte) 0xAA}
            ),
            software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {(byte) 0xBB}
            )
        ));

        var result = task.objectFrom(attribute);

        assertThat(result, org.hamcrest.Matchers.instanceOf(List.class));

        var encoded = (List<?>) result;

        assertThat(encoded.size(), is(2));
        assertThat(
            java.util.Set.copyOf(encoded),
            is(java.util.Set.of("qg==", "uw=="))
        );
    }


    @Test
    void shouldReadEmptyBinaryWithoutReturningNull() {
        var attribute = AttributeValue.fromB(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[0])
        );

        var result = task.objectFrom(attribute);

        assertThat(result, is(""));
    }


    @Test
    void shouldReadBinaryInsideNestedMapsAndLists() {
        var binary = AttributeValue.fromB(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {(byte) 0xFF, 0}
            )
        );

        var binarySet = AttributeValue.fromBs(List.of(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {0}
            ),
            software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {(byte) 0xFE}
            )
        ));

        var nestedBinary = AttributeValue.fromB(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(
                new byte[] {1, 2}
            )
        );

        var attribute = AttributeValue.fromM(java.util.Map.of(
            "payload", binary,
            "items", AttributeValue.fromL(List.of(
                binarySet,
                AttributeValue.fromM(java.util.Map.of(
                    "nested", nestedBinary
                ))
            ))
        ));

        var result = task.objectFrom(attribute);

        assertThat(
            result,
            is(java.util.Map.of(
                "payload", "/wA=",
                "items", List.of(
                    List.of("AA==", "/g=="),
                    java.util.Map.of("nested", "AQI=")
                )
            ))
        );
    }

}
