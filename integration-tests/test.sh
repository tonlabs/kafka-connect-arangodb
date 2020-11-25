#!/bin/sh
set -e

ACCOUNT="-1:3333333333333333333333333333333333333333333333333333333333333333"
# It must include double quotes on both sides for connector decoding
ENCODED_ACCOUNT_ADDRESS='Ii0xOjMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMi'
ARANGODB_ADDRESS="arangodb:8529"
TEST_BLOCKS_DIR="./kafka-messages"
# connector config file 
CONNECTOR=./connector-under-the-test/arangodb-accounts-sink.properties

query_lastr_transaction_logical_time() {
  local DATA="$(curl http://$ARANGODB_ADDRESS/_db/blockchain/_api/document/testaccounts/$ACCOUNT)"
  echo $DATA | jq -r '.last_trans_lt'
}

rebuild_testaccounts_arangodb_collection() {
  curl -X DELETE --header 'accept: application/json' --dump - http://$ARANGODB_ADDRESS/_db/blockchain/_api/collection/testaccounts && true
  curl -X POST --header 'accept: application/json' --data-binary @- --dump - http://$ARANGODB_ADDRESS/_db/blockchain/_api/collection <<EOF
{
  "name" : "testaccounts"
}
EOF
}

post_update() {
  local SOURCE_DATA_FILE=$1
  local FILENAME=$ENCODED_ACCOUNT_ADDRESS
  cp $SOURCE_DATA_FILE $FILENAME 
  ./send-block --block $FILENAME --rate=0 --config=kafka-publisher-config.json
  #TODO: Hack. Wait data to be processed
  sleep 15s
}

prepate_connector_to_arangodb() {
  local CONNECTOR_FILENAME=$(basename -- "$CONNECTOR") 
  local CONNECTOR_NAME="${CONNECTOR_FILENAME%.*}"
  curl --request POST -H "Content-Type: application/json" --data @connector-config.json http://connect:8083/connectors
  curl -v -X POST  http://connect:8083/connectors/${CONNECTOR_NAME}/restart
}

expect_state() {
  local EXPECTED_STATE=$1 
  local EXPECTED_LAST_TRANSACTION_LOGICAL_TIME="$(cat $EXPECTED_STATE | jq -r '.last_trans_lt')"
  local ACTUAL_STATE="$(query_lastr_transaction_logical_time)"
  if [ "$EXPECTED_LAST_TRANSACTION_LOGICAL_TIME" != "$ACTUAL_STATE" ]; then
    echo "Expected last_trans_lt $EXPECTED_LAST_TRANSACTION_LOGICAL_TIME, but was $ACTUAL_STATE."
    exit 1
  fi
}

step() {
  echo -n " - $1..."
  shift
  $@
  echo "done."
}

echo "Prepare test run:"
step "Cleanup ArangoDB tests collection" rebuild_testaccounts_arangodb_collection
# step "Cleanup Kafka topic with old messages" delete_testaccounts_kafka_topic
step "Prepare test connector" prepate_connector_to_arangodb 
echo "done."

echo "Starting test"
step "Post initial state" post_update $TEST_BLOCKS_DIR/initial-state.json
step "Verify that the initial state in the database" expect_state $TEST_BLOCKS_DIR/initial-state.json
step "Send an outdated update" post_update $TEST_BLOCKS_DIR/outdated.json
step "Verify it didn't update the state" expect_state $TEST_BLOCKS_DIR/initial-state.json
step "Post an actual update" post_update $TEST_BLOCKS_DIR/state-update.json
step "Verify the state was updated" expect_state $TEST_BLOCKS_DIR/state-update.json
echo "all done."
echo "Test passed."
exit 0
