# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster
RUN apt-get update && apt-get install -y git openssh-client g++ unixodbc-dev
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
WORKDIR /app
COPY requirements.txt requirements.txt
RUN --mount=type=ssh pip3 install -r requirements.txt
COPY . .

ENTRYPOINT ["python3", "-m", "gerald_syncing"]