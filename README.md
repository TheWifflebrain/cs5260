# cs5260
cs5260 aws homework which is uploading files to buckets, and dynamodb tables

to run consumer.py:
python -u consumer.py b_usu-cs5260-wasatch-requests b_usu-cs5260-wasatch-web

put above statement into the terminal 
first argument is: where the requests files are at that you want to read
seconds argument is: b for bucket or d for database followed by an underscore and then the table name where you want to put the requests

to run test_consumer.py:
pytest

put the above statement in the terminal
