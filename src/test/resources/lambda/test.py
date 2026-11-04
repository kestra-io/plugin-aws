import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    print("Lambda function ARN:", context.invoked_function_arn)
    action = event.get("action")
    if action is not None:
        if action == "error":
            logger.info("Will throw an Exception. Action: " + action)
            raise Exception("Error for client tests")
        if action == "sleep":
            # a slow function, for the client's read timeout (see InvokeTest)
            seconds = event.get("seconds", 1)
            logger.info("Will sleep " + str(seconds) + "s")
            time.sleep(seconds)
        logger.info("Normal work - Unknown action: " + action)
    else:
        logger.info("Normal work - All OK!")

    return {"message": "All OK!", "action": action}
