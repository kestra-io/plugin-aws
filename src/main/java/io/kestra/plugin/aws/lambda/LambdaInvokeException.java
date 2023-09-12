package io.kestra.plugin.aws.lambda;

import java.io.IOException;
import software.amazon.awssdk.services.lambda.model.LambdaException;

/**
 * AWS Lambda invocation exception caused by the function failure or a problem of handling its response. 
 */
public class LambdaInvokeException extends RuntimeException {

    /**
     * Instantiate the LambdaInvokeException with context message and the caused {@link LambdaException}.
     * 
     * @param message String context message
     * @param cause LambdaException the original exception
     */
    LambdaInvokeException(String message, LambdaException cause) {
        super(message, cause);
    }

    /**
     * Instantiate the LambdaInvokeException with context message and the caused {@link IOException}.
     * 
     * @param message String context message
     * @param cause IOException the original exception
     */
    LambdaInvokeException(String message, IOException cause) {
        super(message, cause);
    }

    /**
     * Instantiate the LambdaInvokeException with context message.
     * 
     * @param message String context message
     */
    LambdaInvokeException(String message) {
        super(message);
    }
    
}
