# Ref Seeker Extractor

This service extracts inclusion dependencies from different databases (polystores) and is implemented in Python.

## Contents
- [Ref Seeker Extractor](#ref-seeker-extractor)
  - [Contents](#contents)
  - [Quick Start](#quick-start)
  - [API-Docs](#api-docs)
    - [Example for the `/extract` request](#example-for-the-extract-request)


## Quick Start

The docker compose file with databases containing test data are already included (have a look at the [unibench-ind-data-generator](https://github.com/dbishagen/unibench-ind-data-generator) project).

To start the services and databases with test data, run the following command:

```bash
DATA_SIZE=0.0 docker compose -f docker-compose.yml -f docker-compose-test-data.yml up
```

All general settings are configured in the `ref-seeker-extractor/settings.yaml` file.


We also provide a simple client to test the service. The client is implemented in Go. The code can be found in the `ref-seeker-extractor-client` directory.


To run the client using the provided docker image, execute the following command:
```bash
docker run --rm \
--network=schema-extraction-network \
ghcr.io/dbishagen/ref-seeker-extractor-client:latest
```

To change the configuration like the database connection, you can mount the `conf` directory to the container:

```bash
docker run --rm \
--network=schema-extraction-network \
--mount type=bind,src=$(pwd)/conf,dst=/usr/local/ref-seeker-client/conf,readonly  \
ghcr.io/dbishagen/ref-seeker-extractor-client:latest
```

To clear the database containing the found inclusion dependencies, you can run the following command:

```bash
echo "DROP DATABASE IF EXISTS refseeker;CREATE DATABASE refseeker;" | \
docker compose exec -T mariadb mariadb --user=root --password=refseeker
```


## API-Docs

The API documentation is generated with FastAPI and can be found at [http://localhost:8001/docs](http://localhost:8001/docs).


### Example for the `/extract` request

The following command will show an example of the request body for every supported database system:

```json
{
  "entries": [
    {
      "uri": "mongodb://127.0.0.1:27017/database_name",
      "user": "username",
      "password": "password"
    },
    {
      "uri": "cassandra://127.0.0.1:9042/database_name",
      "user": "username",
      "password": "password"
    },
    {
      "uri": "postgresql://127.0.0.1:5432/database_name",
      "user": "username",
      "password": "password"
    },
    {
      "uri": "neo4j://127.0.0.1:7687",
      "user": "username",
      "password": "password"
    }
  ]
}

```

If an extraction job was started, the `/jobs/<JOB-ID>` request can be used to get the status of the job.
If the job is finished, the results can be retrieved with the `/jobs/<JOB-ID>/results` request.

