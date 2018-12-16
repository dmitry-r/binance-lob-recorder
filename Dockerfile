FROM python:3

LABEL maintainer="rogozhnikov.dmitriy@gmail.com"

RUN mkdir /app
WORKDIR /app

COPY Pipfile Pipfile.lock /app/

RUN pip install pipenv

RUN pipenv install --system --deploy

COPY . /app
