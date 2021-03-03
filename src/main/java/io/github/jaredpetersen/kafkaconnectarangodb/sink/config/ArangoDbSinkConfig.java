package io.github.jaredpetersen.kafkaconnectarangodb.sink.config;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArangoDbSinkConfig extends AbstractConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDbSinkConfig.class);

  private static final String ARANGODB_HOST = "arangodb.host";
  private static final String ARANGODB_HOST_DOC = "ArangoDb server host.";
  public final String arangoDbHost;

  private static final String ARANGODB_PORT = "arangodb.port";
  private static final String ARANGODB_PORT_DOC = "ArangoDb server host port number.";
  public final int arangoDbPort;

  private static final String ARANGODB_USER = "arangodb.user";
  private static final String ARANGODB_USER_DOC = "ArangoDb connection username.";
  public final String arangoDbUser;

  private static final String ARANGODB_PASSWORD = "arangodb.password";
  private static final String ARANGODB_PASSWORD_DEFAULT = "";
  private static final String ARANGODB_PASSWORD_DOC = "ArangoDb connection password.";
  public final Password arangoDbPassword;

  private static final String ARANGODB_USE_SSL = "arangodb.useSsl";
  private static final boolean ARANGODB_USE_SSL_DEFAULT = false;
  private static final String ARANGODB_USE_SSL_DOC = "ArangoDb use SSL connection.";
  public final boolean arangoDbUseSsl;

  private static final String ARANGODB_DATABASE_NAME = "arangodb.database.name";
  private static final String ARANGODB_DATABASE_NAME_DOC = "ArangoDb database name.";
  public final String arangoDbDatabaseName;

  private static final String ARANGODB_OBJECT_UPGRADE_FIELD = "arangodb.object.upgrade";
  private static final String ARANGODB_OBJECT_UPGRADE_FIELD_DEFAULT = "";
  private static final String ARANGODB_OBJECT_UPGRADE_FIELD_DOC = "If set: the value is used as a field name in the document to determine if object update is needed. This field is expected to be an ascending value.";
  public final String arangoDbObjectUpsertFieldFilter;
  
  private static final String ARANGODB_MAX_BATCH_SIZE = "arangodb.batch.maxSize";
  private static final int ARANGODB_MAX_BATCH_SIZE_DEFAULT = -1;
  private static final String ARANGODB_MAX_BATCH_SIZE_DOC = "Sets the maximum number of documents to be send in one batch.";
  public final int arangoDbMaxBatchSize;

  private static final String ARANGODB_RECORD_ADD_TIMESTAMP = "arangodb.record.add-timestamp";
  private static final boolean ARANGODB_RECORD_ADD_TIMESTAMP_DEFAULT = false;
  private static final String ARANGODB_RECORD_ADD_TIMESTAMP_DOC = "Enable/disable putting additional field with timestamp to every record";
  public final boolean arangoDbRecordAddTimestamp;

  private static final String KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES = "kafka.external-message-ref.max-retries";
  private static final int KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES_DEFAULT = 3; 
  private static final String KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES_DOC = "Set the maximum number of attempts to read data for the kafka message stored externally.";
  public final int kafkaExternalMessagesDataReadMaxTries;

  private static final String KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT = "kafka.external-message-ref.retries-defer-timeout";
  private static final int KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT_DEFAULT = 100;
  private static final String KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT_DOC = "Set timeout in milliseconds between read attempts for externally stored message bodies.";
  public final int kafkaExternalMessagesDataReadRetriesDeferTimeout;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ARANGODB_HOST, Type.STRING, Importance.HIGH, ARANGODB_HOST_DOC)
      .define(ARANGODB_PORT, Type.INT, Importance.HIGH, ARANGODB_PORT_DOC)
      .define(ARANGODB_USER, Type.STRING, Importance.HIGH, ARANGODB_USER_DOC)
      .define(ARANGODB_PASSWORD, Type.PASSWORD, ARANGODB_PASSWORD_DEFAULT, Importance.HIGH, ARANGODB_PASSWORD_DOC)
      .define(ARANGODB_USE_SSL, Type.BOOLEAN, ARANGODB_USE_SSL_DEFAULT, Importance.HIGH, ARANGODB_USE_SSL_DOC)
      .define(ARANGODB_DATABASE_NAME, Type.STRING, Importance.HIGH, ARANGODB_DATABASE_NAME_DOC)
      .define(ARANGODB_OBJECT_UPGRADE_FIELD, Type.STRING, ARANGODB_OBJECT_UPGRADE_FIELD_DEFAULT, Importance.HIGH, ARANGODB_OBJECT_UPGRADE_FIELD_DOC)
      .define(ARANGODB_MAX_BATCH_SIZE, Type.INT, ARANGODB_MAX_BATCH_SIZE_DEFAULT,  Importance.HIGH, ARANGODB_MAX_BATCH_SIZE_DOC)
      .define(ARANGODB_RECORD_ADD_TIMESTAMP, Type.BOOLEAN, ARANGODB_RECORD_ADD_TIMESTAMP_DEFAULT, Importance.HIGH, ARANGODB_RECORD_ADD_TIMESTAMP_DOC)
      .define(KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES, Type.INT, KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES_DEFAULT, Importance.HIGH, KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES_DOC)
      .define(KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT, Type.INT, KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT_DEFAULT, Importance.HIGH, KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT_DOC)  
      ;

  /**
   * Configuration for ArangoDB Sink.
   * @param originals configurations.
   */
  public ArangoDbSinkConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, false);

    LOGGER.info("initial config: {}", originals);

    this.arangoDbHost = getString(ARANGODB_HOST);
    this.arangoDbPort = getInt(ARANGODB_PORT);
    this.arangoDbUser = getString(ARANGODB_USER);
    this.arangoDbPassword = getPassword(ARANGODB_PASSWORD);
    this.arangoDbUseSsl = getBoolean(ARANGODB_USE_SSL);
    this.arangoDbDatabaseName = getString(ARANGODB_DATABASE_NAME);
    this.arangoDbObjectUpsertFieldFilter = getString(ARANGODB_OBJECT_UPGRADE_FIELD);
    this.arangoDbMaxBatchSize = getInt(ARANGODB_MAX_BATCH_SIZE);
    this.arangoDbRecordAddTimestamp = getBoolean(ARANGODB_RECORD_ADD_TIMESTAMP);
    int kafkaExternalMessagesDataReadMaxTries = getInt(KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES);
    if (kafkaExternalMessagesDataReadMaxTries < 1) {
       LOGGER.warn(KAFKA_EXTERNAL_MESSAGE_DATA_READ_MAX_TRIES + " is less than 1. Ignoring configured value and setting it to the minimum value of 1.");
       kafkaExternalMessagesDataReadMaxTries = 1;
    }
    this.kafkaExternalMessagesDataReadMaxTries = kafkaExternalMessagesDataReadMaxTries;
    this.kafkaExternalMessagesDataReadRetriesDeferTimeout = getInt(KAFKA_EXTERNAL_MESSAGE_DATA_READ_RETRIES_DEFER_TIMEOUT);
  }
}
