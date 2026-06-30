package io.kestra.plugin.aws.bedrock;

import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.bedrockruntime.model.*;

/**
 * Shared utilities for building Bedrock Converse API request components,
 * extracted to avoid duplication between {@link Converse} and {@link ConverseStream}.
 */
final class BedrockUtils {

    private BedrockUtils() {
    }

    /**
     * Converts a rendered list of {@code {role, content}} maps into SDK {@link Message} objects.
     *
     * @throws IllegalArgumentException if any entry has an unrecognised role, a missing role,
     *         or a missing content value.
     */
    static List<Message> buildMessages(List<Map<String, String>> rawMessages) {
        return rawMessages.stream().map((msg) ->
        {
            var index = rawMessages.indexOf(msg);

            var role = msg.get("role");
            if (role == null || role.isBlank()) {
                throw new IllegalArgumentException(
                    "Message at index " + index + " is missing required field 'role'."
                );
            }

            ConversationRole sdkRole;
            if ("user".equalsIgnoreCase(role)) {
                sdkRole = ConversationRole.USER;
            } else if ("assistant".equalsIgnoreCase(role)) {
                sdkRole = ConversationRole.ASSISTANT;
            } else {
                throw new IllegalArgumentException(
                    "Message at index " + index + " has unsupported role '" + role +
                        "'. Allowed values: 'user', 'assistant'."
                );
            }

            var content = msg.get("content");
            if (content == null) {
                throw new IllegalArgumentException(
                    "Message at index " + index + " is missing required field 'content'."
                );
            }

            return Message.builder()
                .role(sdkRole)
                .content(ContentBlock.fromText(content))
                .build();
        }).toList();
    }

    /**
     * Builds an {@link InferenceConfiguration} from a rendered config map.
     *
     * @throws IllegalArgumentException if a key has the wrong value type.
     */
    static InferenceConfiguration buildInferenceConfig(Map<String, Object> ic) {
        var icBuilder = InferenceConfiguration.builder();
        if (ic.containsKey("maxTokens")) {
            try {
                icBuilder.maxTokens(((Number) ic.get("maxTokens")).intValue());
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                    "inferenceConfig.maxTokens must be an integer, got: " + ic.get("maxTokens"), e
                );
            }
        }
        if (ic.containsKey("temperature")) {
            try {
                icBuilder.temperature(((Number) ic.get("temperature")).floatValue());
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                    "inferenceConfig.temperature must be a number between 0 and 1, got: " + ic.get("temperature"), e
                );
            }
        }
        if (ic.containsKey("topP")) {
            try {
                icBuilder.topP(((Number) ic.get("topP")).floatValue());
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                    "inferenceConfig.topP must be a number between 0 and 1, got: " + ic.get("topP"), e
                );
            }
        }
        if (ic.containsKey("stopSequences")) {
            try {
                @SuppressWarnings("unchecked")
                var stops = (List<String>) ic.get("stopSequences");
                icBuilder.stopSequences(stops);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                    "inferenceConfig.stopSequences must be a list of strings, got: " + ic.get("stopSequences"), e
                );
            }
        }
        return icBuilder.build();
    }
}
