#!/usr/bin/env bash

set -o nounset
set -o errexit

trap 'echo "Aborting due to errexit on line $LINENO. Exit code: $?" >&2' ERR

set -o errtrace

set -o pipefail

IFS=$'\n\t'

_ME="$(basename "${0}")"




if [ -f ./.env ]; then
    source .env
else 
    cp templates/env.template .env
    echo '.env not found; created, edit and try again'
    exit 1
fi

podman run --rm -d --name pemex-minio -e "MINIO_ROOT_USER=$MINIO_ACCESS_KEY" -e "MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY" -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address :9001
podman run --rm -d --name pemex-redis -p 6379:6379 redis
hatch run pemex --article-archiver --stream redis --s3-conn minio
