package io.github.jaredpetersen.kafkaconnectarangodb.sink.config;

import java.util.List;
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

  private static final String CONNECTION_URL = "connection.url";
  private static final String CONNECTION_URL_DOC = "Connection url.";

  private static final String CONNECTION_JWT = "connection.jwt";
  private static final String CONNECTION_JWT_DOC = "Connection JSON Web Token. Must be superuser in order to tail the Write-Ahead Log.";

  private static final String DB_NAME = "db.name";
  private static final String DB_NAME_DOC = "Comma-separated list of database names.";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CONNECTION_URL, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC)
      .define(CONNECTION_JWT, Type.PASSWORD, Importance.HIGH, CONNECTION_JWT_DOC)
      .define(DB_NAME, Type.LIST, Importance.HIGH, DB_NAME_DOC);

  /**
   * Configuration for ArangoDB Sink.
   * @param originals configurations.
   */
  public ArangoDbSinkConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals, false);

    LOGGER.info("initial config: {}", originals);
  }

  public String getConnectionUrl() {
    return getString(CONNECTION_URL);
  }

  public Password getConnectionJwt() {
    return getPassword(CONNECTION_JWT);
  }

  public List<String> getDatabaseNames() {
    return getList(DB_NAME);
  }
}
