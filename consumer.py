#https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItemsDocumentClasses.html
import boto3
import sys
import time
import json
from types import SimpleNamespace
from operator import itemgetter

s3 = boto3.resource('s3')
client = boto3.client('s3')
read_bucket = s3.Bucket('usu-cs5260-wasatch-dist')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table("widgets")

def prepare_data(obj):
    body = obj.get()['Body'].read()
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
    j_data = json.loads(body.decode('utf8').replace("'", '"'))
    j_data_serialized = json.dumps(j_data, indent=4, sort_keys=True)
    return j_data_serialized

def get_5ordered_requests(files):
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
            if(count_requests == 5):
                break
    # delete key pair
    if(len(requests) >= 1):
        sorted(requests, key=itemgetter(1))
        requests = [item[0] for item in requests]

    return requests, len(requests)

if __name__ == '__main__':
    storage_strategy = sys.argv[1]
    resources_to_use = sys.argv[2]
    tries = 0

    while tries < 10:
        files = read_bucket.objects.all()
        # 5 sorted requests 
        # in instructions: read Widget Requests from Bucket 2 in key order
        requests, num_requests = get_5ordered_requests(files)
        if(num_requests > 0):
            tries = 0
            for obj in requests:
                key = str(obj.key)
                body, json_data, owner = prepare_data(obj)
                # blank requests
                if body != -1 and json_data != -1 and owner != -1:
                    # insert bucket
                    if(storage_strategy == 'bucket'):
                        j_data_serialized = prepare_s3bucket_data(body)
                        client.put_object(
                            Body=j_data_serialized,
                            Bucket='usu-cs5260-wasatch-dist',
                            Key=f'widgets/{owner}/{json_data.widgetId}'
                        )

                    # insert db
                    if(storage_strategy == 'dynamodb'):
                        item = prepare_dynamodb_data(json_data, owner)
                        table.put_item(Item=item)

                # delete
                client.delete_object(Bucket='usu-cs5260-wasatch-dist', Key=key)
        else:
            time.sleep(0.1)
            tries+=1

