import json
import logging
import boto3

sqs = boto3.client('sqs')

def lambda_handler(event, context):
    sqs_qs = sqs.list_queues()
    sqs_q = str(sqs_qs['QueueUrls'][0])
    logging.info(f"widget-request-handler sqs queue: {sqs_q}.")
    body = event['body']
    if(len(body) > 0):
        request_string_for_sqs = body.replace("'", "\"")
        body_json =  json.loads(request_string_for_sqs)
        
        if 'type' not in body_json:
            logging.info("no type in body in widget-request-handler.")
            return {
            'statusCode': 200,
            'body': "Request not processed due to no type of request."
        }
        req_type = body_json['type']
        logging.info(f"the type of the request is: {req_type}.")

        try:
            sqs.send_message(QueueUrl=sqs_q, MessageBody=request_string_for_sqs)
            logging.info("Put request into queue.")
        except Exception:
            logging.info("Could not put request into SQS from lambda.")
            raise Exception
        
        return_msg = f"{req_type} request created: {json.dumps(body)}"
        return {
            'statusCode': 200,
            'body': return_msg
        }
    
    else:
        logging.info("Request was empty.")
        return {
            'statusCode': 200,
            'body': "Request not processed due to no valid request (empty body)."
        }
