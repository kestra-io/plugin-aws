package io.kestra.plugin.aws.ecr;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.AuthorizationData;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;

import java.util.Base64;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.*;

@KestraTest
class GetAuthTokenTest {
    @Inject
    RunContextFactory runContextFactory;

    @Test
    void run_mocked() throws Exception {
        RunContext runContext = runContextFactory.of();

        EcrClient ecrClient = mock(EcrClient.class);

        String encoded = Base64.getEncoder().encodeToString("AWS:my-token".getBytes());

        when(ecrClient.getAuthorizationToken())
            .thenReturn(
                GetAuthorizationTokenResponse.builder()
                    .authorizationData(
                        List.of(AuthorizationData.builder()
                            .authorizationToken(encoded)
                            .build()
                        )
                    )
                    .build()
            );

        GetAuthToken task = spy(GetAuthToken.builder()
            .region(io.kestra.core.models.property.Property.ofValue("eu-west-3"))
            .build());

        doReturn(ecrClient).when(task).client(any(RunContext.class));

        GetAuthToken.TokenOutput output = task.run(runContext);

        assertThat(output, notNullValue());
        assertThat(output.getToken(), notNullValue());

        verify(ecrClient).getAuthorizationToken();
    }
}
