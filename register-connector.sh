#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Registering Debezium Connector from register-connector.json..."

curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d "@${SCRIPT_DIR}/register-connector.json"

echo -e "\nConnector registration request sent."
