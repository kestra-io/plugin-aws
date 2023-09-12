package io.kestra.plugin.aws.lambda;

import java.io.InputStream;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.CreateFunctionRequest;
import software.amazon.awssdk.services.lambda.model.CreateFunctionResponse;
import software.amazon.awssdk.services.lambda.model.FunctionCode;
import software.amazon.awssdk.services.lambda.model.GetFunctionRequest;
import software.amazon.awssdk.services.lambda.model.GetFunctionResponse;
import software.amazon.awssdk.services.lambda.model.Runtime;
import software.amazon.awssdk.services.lambda.waiters.LambdaWaiter;

@MicronautTest
@Testcontainers
public class AbstractInvokeTest {

    static final String FUNCTION_NAME = "Test-Lambda";

    static final String FUNCTION_ROLE_ARN = "arn:aws:iam::000000000000:role/lambda-role";

    static final String FUNCTION_CODE = "lambda/test.py.zip";

    protected static LocalStackContainer localstack;

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    protected String functionArn;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.3.1"))
                .withServices(LocalStackContainer.Service.LAMBDA);
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    void createFunction(LambdaClient client) {
        if (client.listFunctions().functions().stream()
                .filter(config -> config.functionName().equals(FUNCTION_NAME))
                .collect(Collectors.toList()).size() == 0) {
            LambdaWaiter waiter = client.waiter();

            InputStream is = getClass().getClassLoader().getResourceAsStream(FUNCTION_CODE);

            SdkBytes codeToUpload = SdkBytes.fromInputStream(is);

            FunctionCode code = FunctionCode.builder().zipFile(codeToUpload).build();

            CreateFunctionRequest functionRequest = CreateFunctionRequest.builder()
                    .functionName(FUNCTION_NAME).description("Created by the Lambda Java API")
                    .code(code).handler("test.handler")
                    .role(FUNCTION_ROLE_ARN)
                    .runtime(Runtime.PYTHON3_9)
                    .build();

            // Create a Lambda function using a waiter.
            CreateFunctionResponse functionResponse = client.createFunction(functionRequest);
            GetFunctionRequest getFunctionRequest =
                    GetFunctionRequest.builder().functionName(FUNCTION_NAME).build();
            WaiterResponse<GetFunctionResponse> waiterResponse =
                    waiter.waitUntilFunctionExists(getFunctionRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);
            // FYI ARN can be found as follows
            //functionArn = functionResponse.functionArn();
        }
    }


}
