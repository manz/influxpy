#!/usr/bin/env bash

docker-compose up -d influxdb
docker-compose run --rm testinfluxpy
