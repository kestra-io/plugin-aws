package io.kestra.plugin.aws.bedrock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.InferenceConfiguration;
import software.amazon.awssdk.services.bedrockruntime.model.Message;

/**
 * Shared utilities for building Bedrock Converse API request components,
 * extracted to avoid duplication between {@link Converse} and {@link ConverseStream}.
 */
final class BedrockUtils {

    private BedrockUtils() {}

    /**
     * Converts a rendered list of {@code {role, content}} maps into SDK {@link Message} objects.
     *
     * @throws IllegalArgumentException if any entry has an unrecognised role, a missing role,
     *                                  or a missing content value.
     */
    static List<Message> buildMessages(List<Map<String, String>> rawMessages) {
        List<Message> sdkMessages = new ArrayList<>();
        for (int i = 0; i < rawMessages.size(); i++) {
            Map<String, String> msg = rawMessages.get(i);

            String role = msg.get("role");
            if (role == null || role.isBlank()) {
                throw new IllegalArgumentException(
                    "Message at index " + i + " is missing required field 'role'.");
            }

            ConversationRole sdkRole;
            if ("user".equalsIgnoreCase(role)) {
                sdkRole = ConversationRole.USER;
            } else if ("assistant".equalsIgnoreCase(role)) {
                sdkRole = ConversationRole.ASSISTANT;
            } else {
                throw new IllegalArgumentException(
                    "Message at index " + i + " has unsupported role '" + role +
                    "'. Allowed values: 'user', 'assistant'.");
            }

            String content = msg.get("content");
            if (content == null) {
                throw new IllegalArgumentException(
                    "Message at index " + i + " is missing required field 'content'.");
            }

            sdkMessages.add(Message.builder()
                .role(sdkRole)
                .content(ContentBlock.fromText(content))
                .build());
        }
        return sdkMessages;
    }

    /**
     * Builds an {@link InferenceConfiguration} from a rendered config map.
     */
    static InferenceConfiguration buildInferenceConfig(Map<String, Object> ic) {
        InferenceConfiguration.Builder icBuilder = InferenceConfiguration.builder();
        if (ic.containsKey("maxTokens")) {
            icBuilder.maxTokens(((Number) ic.get("maxTokens")).intValue());
        }
        if (ic.containsKey("temperature")) {
            icBuilder.temperature(((Number) ic.get("temperature")).floatValue());
        }
        if (ic.containsKey("topP")) {
            icBuilder.topP(((Number) ic.get("topP")).floatValue());
        }
        if (ic.containsKey("stopSequences")) {
            @SuppressWarnings("unchecked")
            List<String> stops = (List<String>) ic.get("stopSequences");
            icBuilder.stopSequences(stops);
        }
        return icBuilder.build();
    }
}
