import json
import logging
import boto3

sqs = boto3.client('sqs')

def lambda_handler(event, context):
    body = event['body']
    if(len(body) > 0):
        request_string_for_sqs = body.replace("'", "\"")
        body_json =  json.loads(request_string_for_sqs)
        
        if 'type' not in body_json:
            return {
            'statusCode': 200,
            'body': "Request not processed due to no type of request."
        }
        req_type = body_json['type']

        try:
            sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/302817204902/cs5260-requests', MessageBody=request_string_for_sqs)
        except Exception:
            logging.info("Could not put request into SQS from lambda.")
            raise Exception
        
        return_msg = f"{req_type} request created: {json.dumps(body)}"
        return {
            'statusCode': 200,
            'body': return_msg
        }
    
    else:
        return {
            'statusCode': 200,
            'body': "Request not processed due to no valid request (empty body)."
        }
