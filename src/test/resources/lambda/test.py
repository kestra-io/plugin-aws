import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    print("Lambda function ARN:", context.invoked_function_arn)
    action = event.get("action")
    if action is not None:
        if action == "error":
            logger.info("Will throw an Exception. Action: " + action)
            raise Exception("Error for client tests")
        logger.info("Normal work - Unknown action: " + action)
    else:
        logger.info("Normal work - All OK!")
    
    return {
        "message": "All OK!",
        "action": action
    } 