#!/bin/sh
# This script will push changes to docker. 
# Provide it with a version number - if this matches the last version, it
# will overwrite the existing docker image, otherwise it will create a new
# image. In both cases, the latest tag is set to the given version.

# USAGE: ./dockerpush.sh <NAME> <VERSION NO.>
# eg. ./dockerpush.sh gerald-syncing v1.1

# Start ssh agent and add the key you use for github
eval `ssh-agent`
ssh-add ~/.ssh/id_ed25519

# Build image
docker build --ssh default --tag $1 .

# Tag image with both version no and latest tag
docker tag $1:latest lvdocker.azurecr.io/$1:$2
docker tag $1:latest lvdocker.azurecr.io/$1:latest

# Authenticate to lvdocker registry
az login
az acr login --name lvdocker

# Push both images
docker push lvdocker.azurecr.io/$1:$2
docker push lvdocker.azurecr.io/$1:latest
