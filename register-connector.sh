#!/bin/bash

echo "Registering Debezium Postgres 2-Stage Pipeline Connector..."

curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d '{
    "name": "cdc-2stage-pipeline-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "appuser",
      "database.password": "apppass",
      "database.dbname": "appdb",
      "topic.prefix": "app",
      "table.include.list": "public.pessoas,public.emails,public.enderecos,public.telefones,public.outbox_events",
      "provide.transaction.metadata": "true",
      "plugin.name": "pgoutput",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }'

echo -e "\nConnector registration request sent."
