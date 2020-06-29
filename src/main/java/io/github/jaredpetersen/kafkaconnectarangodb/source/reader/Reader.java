package io.github.jaredpetersen.kafkaconnectarangodb.source.reader;

import io.github.jaredpetersen.kafkaconnectarangodb.common.arangodb.ArangoDb;
import io.github.jaredpetersen.kafkaconnectarangodb.common.arangodb.pojo.wal.WalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Reader {
  private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

  private final ArangoDb arangoDb;
  private Long lastTick;

  public Reader(ArangoDb arangoDb) {
    this.arangoDb = arangoDb;
    this.lastTick = null;
  }

  public List<WalEntry> read() {
    final List<WalEntry> walEntries = this.arangoDb.tailWal(lastTick);
    LOG.info("result: {}", walEntries);

    this.lastTick = Long.parseLong(walEntries.get(walEntries.size() - 1).getTick());

    return walEntries;
  }
}
