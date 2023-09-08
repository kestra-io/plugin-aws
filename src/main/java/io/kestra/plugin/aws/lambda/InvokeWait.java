package io.kestra.plugin.aws.lambda;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke Lambda function and wait for its completion."
)
@Plugin(
    examples = {
        @Example(
            title = "Invoke given Lambda function and wait for its completion.",
            code = {
                "functionArn: \"arn:aws:lambda:us-west-2:123456789012:function:my-function\""
            }
        )
    }
)
public class InvokeWait extends AbstractLambdaInvoke  implements RunnableTask<VoidOutput> {

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        try (var lambda = client(runContext)) {
            // Setup an InvokeRequest.
            InvokeRequest request = InvokeRequest.builder().functionName(functionArn)
                    // .payload(payload)
                    .build();

            InvokeResponse res = lambda.invoke(request);
            String value = res.payload().asUtf8String();
            // TODO logging transition states in the plugin
            // TODO use result data in next task, save to the storage etc
            System.out.println(">>> result: " + value);
        } catch (LambdaException e) {
            // TODO logging failures/errors in the plugin
            System.err.println(">>> error:" + e.getMessage());
        }
        return null;
    }

}
