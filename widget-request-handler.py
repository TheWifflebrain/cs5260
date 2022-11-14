import json
import logging
import boto3

sqs = boto3.client('sqs')

def lambda_handler(event, context):
    body = event['body']
    print(type(body))
    print(body)
    
    try:
        sqs.send_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/302817204902/cs5260-requests', MessageBody=body)
    except Exception:
        logging.info("Could not put request into SQS from lambda.")
        raise Exception
    
    print("hello world")
        
    return {
        'statusCode': 200,
        'body': json.dumps(bod)
    }
