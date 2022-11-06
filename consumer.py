import boto3
import sys
import time
import json
from types import SimpleNamespace
from operator import itemgetter
import logging
import os

# getting environment variables
for k, v in os.environ.items():
    if k == "aws_access_key_id":
        paws_access_key_id = v
    if k == "aws_secret_access_key":
        paws_secret_access_key = v
    if k == "aws_session_token":
        paws_session_token = v

# environment variables to the services
s3 = boto3.resource('s3',
        aws_access_key_id=paws_access_key_id,
        aws_secret_access_key= paws_secret_access_key)
client = boto3.client('s3', aws_access_key_id=paws_access_key_id,
        aws_secret_access_key=paws_secret_access_key)
sqs = boto3.resource('sqs', region_name='us-east-1', aws_access_key_id=paws_access_key_id,
        aws_secret_access_key= paws_secret_access_key, aws_session_token=paws_session_token)
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=paws_access_key_id,
        aws_secret_access_key=paws_secret_access_key, aws_session_token=paws_session_token)

def analyze_cl_arguments(argv1, argv2):
    # bucket or queue from where all the requests are at
    resources_to_use = argv1
    type_rtu = resources_to_use[0]
    get_resources_here = resources_to_use[2:]
    # where the requests are going
    # b or d for either bucket or dynamodb for first letter
    # then either bucket name or table name
    # ex. d_tablename
    # ex. b_bucketname
    storage_strategy = argv2
    type_requst = storage_strategy[0]
    put_requests_here = storage_strategy[2:]

    return type_rtu, get_resources_here, type_requst, put_requests_here

# https://docs.aws.amazon.com/code-samples/latest/catalog/python-sqs-message_wrapper.py.html
def receive_queue_messages(queue, max_number, wait_time):
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        return messages
    except:
        logging.info(f'Could NOT recieve messages from queue')
        raise Exception

def prepare_data(body):
    # preparing the data from a request for aws commands
    if len(body) > 0:
        json_data = json.loads(body, object_hook=lambda d: SimpleNamespace(**d))
        owner = (json_data.owner.lower()).replace(" ", "-")
        return body, json_data, owner
    # if get an empty request
    else:
        return -1, -1, -1

def prepare_dynamodb_data(json_data, owner):
    name = []
    value = []
    add_oa = False
    for k in json_data.__dict__:
        if json_data.__dict__[k] is not None:
            if k == 'otherAttributes': add_oa = True
            # not not want these attributes in the table and 
            # otherAttributes need to be stand alone attributes (not as single map or list)
            if k != 'otherAttributes' and k != 'type' and k != 'requestId':
                # widget id needs to be id so put_item function can put the item into
                # dynamodb
                if k == 'widgetId':
                    k = 'id'
                    name.insert(0, k)
                    value.insert(0, json_data.__dict__['widgetId'])
                else: 
                    name.append(k)
                    if k == 'owner': value.append(owner)
                    else: value.append(json_data.__dict__[k])
    if add_oa == True:
        for oa in json_data.otherAttributes:
            name.append(oa.name)
            value.append(oa.value)
    item = {name[i]: value[i] for i in range(len(name))}
    return item

def prepare_s3bucket_data(body):
    # preparing the data how an s3 bucket needs data
    j_data = json.loads(body.decode('utf8').replace("'", '"'))
    j_data_serialized = json.dumps(j_data, indent=4, sort_keys=True)
    return j_data_serialized

def insert_into_bucket(client, j_data_serialized, put_requests_here, owner, widget_id):
    logging.info("Got s3 bucket data (prepare_s3bucket_dataa)")
    try:
        client.put_object(
        Body=j_data_serialized,
        Bucket=put_requests_here,
        Key=f'widgets/{owner}/{widget_id}'
        )
        logging.info("Entered request into bucket (put_object)")
    # raise an expection if there was an error inserting into bucket
    except Exception:
        logging.info('Could NOT put request into bucket (put_object)')
        raise Exception

def insert_into_dynamdb_table(table, item):
    logging.info("Got dynamodb data (prepare_dynamodb_data)")
    try:
        table.put_item(Item=item)
        logging.info("Entered request into dynamodb table (put_item)")
    # raise an expection if there was an error inserting into db
    except Exception:
        logging.info('Could NOT put request into dynamodb table (put_item)')
        raise Exception

def delete_from_bucket(client, resources_to_use, key):
    try:
        client.delete_object(Bucket=resources_to_use, Key=key)
        logging.info("END: Deleted finished requests (delete_object)")
    # raise an expection if there was an error deleting
    except Exception:
        logging.info('END: Could NOT delete finished request (delete_object)')
        raise Exception

def delete_s3bucket_data(put_requests_here, owner, id):
    try:
        logging.info("Deleted request (delete_s3bucket_data)")
        s3.Object(put_requests_here, f'widgets/{owner}/{id}').delete()
    except:
        logging.info(f'Could NOT delete request: widgets/{owner}/{id}')
        raise Exception

def delete_from_dynamdb_table(table, key):
    try:
        logging.info("Deleted request (delete_from_dynamdb_table)")
        table.delete_item(Key={'id': key})
    except:
        logging.info(f'Could NOT delete request: key')
        raise Exception

def delete_from_queue(message):
    try:
        logging.info("Deleted request (delete_from_queue)")
        message.delete()
    except:
        logging.info(f'Could NOT delete message from queue')
        raise Exception


if __name__ == '__main__':
    type_rtu, get_resources_here, type_requst, put_requests_here = analyze_cl_arguments(sys.argv[1], sys.argv[2])
    table = dynamodb.Table(put_requests_here)
    #logging.basicConfig(filename='consumer_logs.log', filemode='w', level=logging.INFO)
    print(type_rtu, get_resources_here, type_requst, put_requests_here)
    print("hello")

    tries = 0

    while tries < 10:
        logging.info(f"Main while loop tries: {tries}")

        if(type_rtu == "b"):
            logging.info(f'Getting requests from bucket')
            read_bucket = s3.Bucket(get_resources_here)
            files = read_bucket.objects.all()
            # 10 sorted requests 
            # in instructions: read Widget Requests from Bucket 2 in key order
            # also do not read them all at once
            # so i will retrieve 10 requests and sort them
            requests = []
            count_requests = 0
            for obj in files:
                count_requests+=1
                key = str(obj.key)
                # do not want "folders" or programs
                if not "." in key and not "/" in key and key.isnumeric() == True:
                    # for sorting on key
                    requests.append([obj, key])
                # only retreiving some requests as per instructions
                if(count_requests == 10):
                    break
            # delete key pair
            num_requests = len(requests)
            if(num_requests >= 1):
                sorted(requests, key=itemgetter(1))
                requests = [item[0] for item in requests]
            logging.info(f'Got requests {num_requests} requests')

        if(type_rtu == "q"):
            logging.info(f'Getting requests from queue')
            queue = sqs.get_queue_by_name(QueueName=get_resources_here)
            requests = receive_queue_messages(queue, 10, 2)
            num_requests = len(requests)

        # if retrieved more than 0 requests (there are requests to take care of)
        if(num_requests > 0):
            tries = 0
            for obj in requests:
                if(type_rtu == "b"):
                    key = str(obj.key)
                    body = obj.get()['Body'].read()
                if(type_rtu == "q"):
                    key = obj.message_id
                    body = obj.body

                # prepare the data for the aws commands
                body, json_data, owner = prepare_data(body)
                logging.info("Prepared data (prepare_data)")
                logging.info(f'Key: {key}')
                # blank requests do not do them
                if body != -1 and json_data != -1 and owner != -1:
                    request_type = getattr(json_data, 'type')
                    logging.info(f'Request type: {request_type}')
                    logging.info(f'WidgetId: {json_data.widgetId}')
                    # insert bucket
                    if(type_requst == 'b'):
                        if(request_type == 'insert'):
                            logging.info(f'Inserting bucket request')
                            # preparing the data for the format the bucket wants it in
                            j_data_serialized = prepare_s3bucket_data(body)
                            # inserting into bucket
                            insert_into_bucket(client, j_data_serialized, put_requests_here, owner, json_data.widgetId)
                        if(request_type == 'delete'):
                            logging.info(f'Deleting bucket request')
                            delete_s3bucket_data(put_requests_here, owner, json_data.widgetId)
                        if(request_type == 'update'):
                            logging.info(f'Updating bucket request')
                            delete_s3bucket_data(put_requests_here, owner, json_data.widgetId)
                            j_data_serialized = prepare_s3bucket_data(body)
                            insert_into_bucket(client, j_data_serialized, put_requests_here, owner, json_data.widgetId)
                    # insert db
                    if(type_requst == 'd'):
                        print("hello1")
                        if(request_type == 'insert'):
                            print("hello2")
                            logging.info(f'Updating dynamodb request')
                            # preparing the data for the format the db wants it in
                            item = prepare_dynamodb_data(json_data, owner)
                            # inserting into db
                            insert_into_dynamdb_table(table, item)
                        if(request_type == 'delete'):
                            print("hello3")
                            logging.info(f'Deleting dynamodb request')
                            delete_from_dynamdb_table(table, json_data.widgetId)
                        if(request_type == 'update'):
                            print("hello4")
                            logging.info(f'Updating dynamodb request')
                            delete_from_dynamdb_table(table, json_data.widgetId)
                            item = prepare_dynamodb_data(json_data, owner)
                            insert_into_dynamdb_table(table, item)
                        
                #delete
                if(type_rtu == "b"):
                    delete_from_bucket(client, get_resources_here, key)
                if(type_rtu == "q"):
                    delete_from_queue(obj)
                # info for logging
                logging.info(f'Finished: {key}')
                logging.info("Finished one loop")

        else:
            time.sleep(0.1)
            tries+=1
            logging.info(f"Else statement tries: {tries}")
    logging.info("Finished")
    
