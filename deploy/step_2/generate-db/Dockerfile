FROM python:3.7-alpine

RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev git postgresql-client

ADD . /app

WORKDIR /app

RUN pip3 install -r /app/requirements.txt

ENTRYPOINT [ "sh", "generate.sh" ]