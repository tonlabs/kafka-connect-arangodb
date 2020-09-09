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

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ARANGODB_HOST, Type.STRING, Importance.HIGH, ARANGODB_HOST_DOC)
      .define(ARANGODB_PORT, Type.INT, Importance.HIGH, ARANGODB_PORT_DOC)
      .define(ARANGODB_USER, Type.STRING, Importance.HIGH, ARANGODB_USER_DOC)
      .define(ARANGODB_PASSWORD, Type.PASSWORD, ARANGODB_PASSWORD_DEFAULT, Importance.HIGH, ARANGODB_PASSWORD_DOC)
      .define(ARANGODB_USE_SSL, Type.BOOLEAN, ARANGODB_USE_SSL_DEFAULT, Importance.HIGH, ARANGODB_USE_SSL_DOC)
      .define(ARANGODB_DATABASE_NAME, Type.STRING, Importance.HIGH, ARANGODB_DATABASE_NAME_DOC)
      .define(ARANGODB_OBJECT_UPGRADE_FIELD, Type.STRING, ARANGODB_OBJECT_UPGRADE_FIELD_DEFAULT, Importance.HIGH, ARANGODB_OBJECT_UPGRADE_FIELD_DOC)
      .define(ARANGODB_MAX_BATCH_SIZE, Type.INT, ARANGODB_MAX_BATCH_SIZE_DEFAULT,  Importance.HIGH, ARANGODB_MAX_BATCH_SIZE_DOC)
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
  }
}
