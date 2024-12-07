FROM python:3.12-slim

RUN pip3 install --no-cache-dir boto3 loguru

RUN mkdir -p ~/.aws

WORKDIR /usr/src/app

COPY . .

CMD ["bash", "-c", "python3 ./pull_inventarioprod.py && python3 ./load_inventarioprod.py"]