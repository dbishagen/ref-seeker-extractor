#!/bin/bash

echo "DROP DATABASE IF EXISTS refseeker;CREATE DATABASE refseeker;" | \
docker compose exec -T mariadb mariadb --user=root --password=refseeker

docker run --rm \
--network=schema-extraction-network \
ghcr.io/dbishagen/ref-seeker-extractor-client:latest
