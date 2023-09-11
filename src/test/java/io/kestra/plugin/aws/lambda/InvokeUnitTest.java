package io.kestra.plugin.aws.lambda;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Optional;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mock.Strictness;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.aws.lambda.Invoke.Output;
import lombok.val;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

@ExtendWith(MockitoExtension.class)
public class InvokeUnitTest {
    
    private Invoke invoke;

    @Mock(strictness = Strictness.LENIENT)
    private RunContext context;

    private File tempFile;

    @BeforeEach
    public void setUp() throws IOException, IllegalVariableEvaluationException {
        given(context.tempFile(anyString())).willReturn(Files.createTempFile("test", "lambdainvoke"));
        given(context.metric(any())).willReturn(context);
        given(context.render(anyString())).willAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                return invocation.getArgument(0, String.class).toString();
            }
        });
        given(context.putTempFile(any(File.class))).willAnswer(new Answer<URI>() {
            @Override
            public URI answer(InvocationOnMock invocation) throws Throwable {
                tempFile = invocation.getArgument(0, File.class);
                return tempFile.toURI();
            }
        });

        invoke = Invoke.builder()
            .functionArn("test_function_arn")
            .functionPayload(null) // w/o paramters now
            .id(InvokeUnitTest.class.getSimpleName())
            .type(InvokeUnitTest.class.getName())
            .accessKeyId("test_accessKeyId")
            .secretKeyId("test_secretKeyId")
            .region("test_region")
            .build();        
    }

    @Test
    public void testParseContentType_NoContentType_Binary() {
        assertEquals(ContentType.APPLICATION_OCTET_STREAM, invoke.parseContentType(Optional.empty()), "Should be binary");
    }
   
    @Test
    public void testParseContentType_BadContent_Binary() {
        assertEquals(ContentType.APPLICATION_OCTET_STREAM, invoke.parseContentType(Optional.of("fake/type")), "Should be binary");
    }
    
    @Test
    public void testParseContentType_JSON() {
        assertEquals(ContentType.APPLICATION_JSON.getMimeType().toString(),
                invoke.parseContentType(Optional.of("application/json")).getMimeType().toString(),
                "Should be JSON");
    }

    @Test
    public void testReadError_NotJsonType(@Mock SdkBytes bytes) {
        assertThrows(LambdaInvokeException.class, () -> {
            invoke.handleError(invoke.getFunctionArn(), ContentType.APPLICATION_OCTET_STREAM, bytes);
            }, "Should throw an error"
        );
    }

    @Test
    public void testReadError_FromJsonMessage(@Mock SdkBytes bytes) {
        String errorText = "'path'";
        given(bytes.asUtf8String()).willReturn(
                "{\"errorMessage\": \"" + errorText + "\", \"errorType\": \"KeyError\"}");
        Throwable throwable = assertThrows(LambdaInvokeException.class, () -> {
            invoke.handleError(invoke.getFunctionArn(), ContentType.APPLICATION_JSON, bytes);
        }, "Should throw an error");
        assertTrue(throwable.getMessage().indexOf(errorText) > 0,
                "Exception message should contain an original message");
    }

    @Test
    public void testHandleContent_SaveFile_ReturnOutput(@Mock SdkBytes bytes) throws IOException {
        //val json = "{\"data\": \"some value\", \"author\": \"The User\"}";
        val data = "some raw data";
        given(bytes.asInputStream()).willReturn(new ByteArrayInputStream(data.getBytes()));

        Output res = invoke.handleContent(context, invoke.getFunctionArn(), ContentType.APPLICATION_OCTET_STREAM, bytes);

        checkOutput(data, res);
    }

    private void checkOutput(final String originalData, final Output result) throws IOException {
        assertNotNull(tempFile);
        val savedData = new String(Files.readAllBytes(tempFile.toPath()));
        assertEquals(originalData, savedData, "Data must match");

        assertNotNull(result, "Output should be returned");
        assertEquals(tempFile.toURI(), result.getUri(), "Content URI must mach");
        assertEquals(originalData.length(), result.getContentLength(), "Content length must mach");
        assertEquals(ContentType.APPLICATION_OCTET_STREAM.toString(), result.getContentType(),
                "Content type must mach");
    }

    // ******** BDD usecases ******** 
    @Test
    public void givenFunctionArnNoParams_whenInvokeLambda_thenOutputWithFile(
                @Mock LambdaClient awsLambda,
                @Mock InvokeResponse awsResponse,
                @Mock SdkHttpResponse awsHttpResponse,
                @Mock SdkBytes payload) 
                throws IllegalVariableEvaluationException, IOException {
        //Given: functionArn and no input params, AWS Lambda clinet mocked for the expected behaviour
        val data = "some raw data";
        given(payload.asInputStream()).willReturn(new ByteArrayInputStream(data.getBytes()));
        given(awsHttpResponse.firstMatchingHeader(eq("Content-Type"))).willReturn(Optional.of(ContentType.APPLICATION_OCTET_STREAM.getMimeType()));
        given(awsResponse.functionError()).willReturn(null); // means no error
        given(awsResponse.sdkHttpResponse()).willReturn(awsHttpResponse);
        given(awsResponse.payload()).willReturn(payload);
        given(awsLambda.invoke(any(InvokeRequest.class))).willReturn(awsResponse);
        
        // Mock AbstractLambdaInvoke.client() to return the mocked AWS client
        val spyInvoke = spy(invoke);
        doReturn(awsLambda).when(spyInvoke).client(any());
        
        // When 
        Output res = assertDoesNotThrow(() -> {
            return spyInvoke.run(context);
        }, "No exception should be thrown");
        
        // Then
        checkOutput(data, res);
    }
}