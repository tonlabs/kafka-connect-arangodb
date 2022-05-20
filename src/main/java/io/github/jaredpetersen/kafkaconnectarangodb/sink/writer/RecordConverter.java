package io.github.jaredpetersen.kafkaconnectarangodb.sink.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.System;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import java.net.MalformedURLException;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.errors.ExternalMessageDataMalformedURLException;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.ArangoDbSinkTask;

/**
 * Convert Kafka Connect records to ArangoDB records.
 */
public class RecordConverter {
  public final String EXTERNAL_MESSAGE_DATA_HEADER_KEY = "external-message-ref";

  //private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);
  private static final Logger LOG = LoggerFactory.getLogger(ArangoDbSinkTask.class);

  private final JsonConverter jsonConverter;
  private final JsonDeserializer jsonDeserializer;
  private final ObjectMapper objectMapper;
  private final int kafkaExternalMessagesDataReadMaxTries;
  private final int kafkaExternalMessagesDataReadRetriesDeferTimeout;
  private final boolean addTimestampToArangoRecords;
  private final Map<String, String> collectionMapping;

  /**
   * Construct a new RecordConverter.
   * @param jsonConverter Utility for serializing SinkRecords
   * @param jsonDeserializer Utility for deserializing serialized SinkRecords to JSON
   * @param objectMapper Utility for writing JSON to a string
   */
  public RecordConverter(final JsonConverter jsonConverter, final JsonDeserializer jsonDeserializer, final ObjectMapper objectMapper) {
    this(jsonConverter, jsonDeserializer, objectMapper, 3, 100, false, new HashMap<String, String>());
  }

  /**
   * Construct a new RecordConverter.
   * @param jsonConverter Utility for serializing SinkRecords
   * @param jsonDeserializer Utility for deserializing serialized SinkRecords to JSON
   * @param objectMapper Utility for writing JSON to a string
   * @param kafkaExternalMessagesDataReadMaxTries Number of maximum attempts to read message body stored externally
   * @param kafkaExternalMessagesDataReadRetriesDeferTimeout Defer timeout between read attempts in ms
   * @param addTimestampToArangoRecords enable/disable additional timestamp field in value of record
   * @param collectionMapping ArangoDb collection name configured by user
   */
  public RecordConverter(final JsonConverter jsonConverter, final JsonDeserializer jsonDeserializer, final ObjectMapper objectMapper, 
          int kafkaExternalMessagesDataReadMaxTries, 
          int kafkaExternalMessagesDataReadRetriesDeferTimeout, 
          boolean addTimestampToArangoRecords,
          Map<String, String> collectionMapping) {
    this.jsonConverter = jsonConverter;
    this.jsonDeserializer = jsonDeserializer;
    this.objectMapper = objectMapper;
    this.kafkaExternalMessagesDataReadMaxTries = kafkaExternalMessagesDataReadMaxTries; 
    this.kafkaExternalMessagesDataReadRetriesDeferTimeout = kafkaExternalMessagesDataReadRetriesDeferTimeout;
    this.addTimestampToArangoRecords = addTimestampToArangoRecords;
    this.collectionMapping = collectionMapping;
  }


  /**
   * Convert SinkRecord to an ArangoRecord.
   * @param record Record to convert
   * @return ArangoRecord equivalent of the SinkRecord
   */
  public final ArangoRecord convert(final SinkRecord record)
     throws ExternalMessageDataMalformedURLException
  {
    final String mapped = this.collectionMapping.get(record.topic());
    final String collection = mapped == null ? this.getCollection(record) : mapped;

    return new ArangoRecord(
      collection,
      this.getKey(record),
      this.getValue(record));
  }

  /**
   * Get the ArangoDB collection name from the SinkRecord.
   * Collection name is always the last item in the SinkRecord topic, split by a period.
   * @param record Record to get the collection name from
   * @return ArangoDB collection name
   */
  private String getCollection(final SinkRecord record) {
    final String topic = record.topic();
    return topic.substring(topic.lastIndexOf(".") + 1);
  }

  /**
   * Get the ArangoDB document key from the SinkRecord.
   * @param record Record to get the document key from
   * @return ArangoDB document key
   */
  @SuppressWarnings("unchecked")
  private String getKey(final SinkRecord record) {
    final String key;

    if (record.keySchema() == null) {
      // Schemaless
      final Map<String, Object> keyStruct = (Map<String, Object>) record.key();
      final String keyField = keyStruct.keySet().iterator().next();

      key = keyStruct.get(keyField).toString();
    } else {
      // Schemaful
      final Struct keyStruct = (Struct) record.key();
      final Field keyField = record.keySchema().fields().get(0);

      key = keyStruct.get(keyField).toString();
    }

    return key;
  }
  
  private String getExternalMessageDataRef(final Headers headers) {
    if (headers == null) {
      LOG.info("Headers count: null");
      return null;
    }
    LOG.info("Headers count: " + headers.size());
    
    final Header dataRef = headers.lastWithName(EXTERNAL_MESSAGE_DATA_HEADER_KEY);
    return dataRef == null ? null : (String)dataRef.value();
  }

  private Object extractExternalMessageData(final String address)
    throws ExternalMessageDataMalformedURLException 
  {
    LOG.info("Extractin external message data from " + address);
    int remainingTries = this.kafkaExternalMessagesDataReadMaxTries;
    try {
      final URL url = new URL(address);
      final BufferedReader inputStream = new BufferedReader(
        new InputStreamReader(url.openStream())
      );
      @SuppressWarnings("unchecked")
      Map<String,Object> result = new ObjectMapper().readValue(inputStream, HashMap.class);
      return result;
    } catch (MalformedURLException e) {
      throw new ExternalMessageDataMalformedURLException(e);
    } catch (IOException e) {
      remainingTries --;
      if (remainingTries <= 0) {
        throw new DataException("Failed to read message data despite retrying", e);
      }
      int deferRetryMs = this.kafkaExternalMessagesDataReadRetriesDeferTimeout;
      LOG.warn(
        String.format(
          "IOExeption in external data (will retry after %dms): %s",
          deferRetryMs,
          e.getMessage()
        )
      );
      try {
        if (deferRetryMs > 0) {
          Thread.sleep(deferRetryMs);
        }
      } catch(InterruptedException ignoredError) {
      }
      
      throw new RetriableException("IOExeption in external data", e);
    }
  }

  /**
   * Get the ArangoDB document value as stringified JSON from the SinkRecord.
   * @param record Record to get the document value from
   * @return ArangoDB document value in stringified JSON
   */
  @SuppressWarnings("unchecked")
  private String getValue(final SinkRecord record) 
    throws ExternalMessageDataMalformedURLException
  {
    // Externam message value
    final String externalMessageDataRef = getExternalMessageDataRef(record.headers()); 
    final Object data = (externalMessageDataRef == null || externalMessageDataRef == "") ? 
      record.value() : extractExternalMessageData(externalMessageDataRef);
    // Tombstone records don't need to be converted
    if (data == null) {
      return null;
    }

    // Get the name and value of the key field so that we can rename it as "_key" later inside of the record value
    final String keyFieldName;
    final String keyValue;

    if (record.keySchema() == null) {
      // Schemaless
      final Map<String, Object> keyStruct = (Map<String, Object>) record.key();
      keyFieldName = keyStruct.keySet().iterator().next();
      keyValue = keyStruct.get(keyFieldName).toString();
    } else {
      // Schemaful
      final Struct keyStruct = (Struct) record.key();
      keyFieldName = record.keySchema().fields().get(0).name();
      keyValue = keyStruct.get(keyFieldName).toString();
    }

    // Convert the record value to JSON
    final byte[] serializedRecord = jsonConverter.fromConnectData(
      record.topic(),
      record.valueSchema(),
      data);
    final JsonNode valueJson = jsonDeserializer.deserialize(record.topic(), serializedRecord);

    // Has to be an object, otherwise we can't write the record to the database
    if (!valueJson.isObject()) {
      throw new IllegalArgumentException("record value is not a single object/document");
    }

    // Include the key in an ArangoDB-format
    final ObjectNode valueJsonObject = (ObjectNode) valueJson;
    valueJsonObject.put("_key", keyValue);
    valueJsonObject.remove(keyFieldName);
    
    // Add timestamp to record
    if(this.addTimestampToArangoRecords){
        valueJsonObject.put("lat_update_ts",System.currentTimeMillis());
    }
    
    // Return the stringified JSON
    try {
      return this.objectMapper.writeValueAsString(valueJsonObject);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("record value cannot be serialized to JSON");
    }
  }
}
