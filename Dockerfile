#syntax=docker/dockerfile:1

FROM python:3.8-slim-buster
RUN apt-get update && apt-get install -y git openssh-client g++ unixodbc-dev curl gnupg
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
WORKDIR /app
COPY requirements.txt requirements.txt
RUN --mount=type=ssh pip3 install -r requirements.txt
COPY . .

#### CHANGE
# Adjust this entrypoint to "module"
ENTRYPOINT ["python3", "-m", "module"]