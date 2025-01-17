package io.github.jaredpetersen.kafkaconnectarangodb.sink;

import com.arangodb.ArangoDB;
import com.arangodb.DbName;
import com.arangodb.ArangoDatabase;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.config.ArangoDbSinkConfig;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.writer.ArangoRecord;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.writer.RecordConverter;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.writer.Writer;
import io.github.jaredpetersen.kafkaconnectarangodb.util.VersionUtil;
import io.github.jaredpetersen.kafkaconnectarangodb.sink.errors.ExternalMessageDataMalformedURLException;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect Task for Kafka Connect ArangoDb Sink.
 */
public class ArangoDbSinkTask extends SinkTask {
  private static final Logger LOG = LoggerFactory.getLogger(ArangoDbSinkTask.class);

  private RecordConverter recordConverter;
  private Writer writer;

  @Override
  public final String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public final void start(final Map<String, String> props) {
    LOG.info("task config: {}", props);

    // Set up database
    final ArangoDbSinkConfig config = new ArangoDbSinkConfig(props);
    final ArangoDB arangodb = new ArangoDB.Builder()
        .host(config.arangoDbHost, config.arangoDbPort)
        .user(config.arangoDbUser)
        .password(config.arangoDbPassword.value())
        .useSsl(config.arangoDbUseSsl)
        .build();
    final ArangoDatabase database = arangodb.db(DbName.of(config.arangoDbDatabaseName));


    // Set up the record converter
    final JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(
        Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"),
        false);

    final JsonDeserializer jsonDeserializer = new JsonDeserializer();
    final ObjectMapper objectMapper = new ObjectMapper();

    this.recordConverter = new RecordConverter(jsonConverter, jsonDeserializer, objectMapper, 
            config.kafkaExternalMessagesDataReadMaxTries,
            config.kafkaExternalMessagesDataReadRetriesDeferTimeout,
            config.arangoDbRecordAddTimestamp,
            config.arangoDbCollectionMapping);
    // Set up the writer
    this.writer = new Writer(
            database, config.arangoDbObjectUpsertFieldFilter, config.arangoDbMaxBatchSize, config.arangoInsertOverwritemode);
  }

  @Override
  public final void put(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    LOG.info("writing {} record(s)...", records.size());

    // Convert sink records into something that can be written
    final Collection<ArangoRecord> arangoRecords = records.stream()
        .map((sinkRecord) -> { 
          try {
            return this.recordConverter.convert(sinkRecord);
          } catch(ExternalMessageDataMalformedURLException e) {
            LOG.error("MalformedURLException in external data reference: {}", e.getMessage());
            throw new DataException("MalformedURLException in external data reference", e);
          }
        })
        .collect(Collectors.toList());

    // Write the ArangoDB records to the database
    this.writer.write(arangoRecords);
  }

  @Override
  public final void stop() {
    // Do nothing
  }
}
