FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install boto3
RUN pip install botocore
RUN pip install certifi
RUN pip install cffi
RUN pip install cryptography
RUN pip install idna
RUN pip install jmespath
RUN pip install pycparser
RUN pip install pyOpenSSL
RUN pip install PySocks
RUN pip install python-dateutil
RUN pip install s3transfer
RUN pip install six
RUN pip install urllib3
RUN pip install win-inet-pton
RUN pip install -r requirements.txt

COPY . .

CMD [ "python3", "consumer.py", "q_cs5260-requests", "d_widgets"]
