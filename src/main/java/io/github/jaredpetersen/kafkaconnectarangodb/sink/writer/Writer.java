package io.github.jaredpetersen.kafkaconnectarangodb.sink.writer;

import com.arangodb.ArangoDatabase;
import com.arangodb.model.AqlFunctionCreateOptions;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.OverwriteMode;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.DocumentDeleteOptions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Writer for writing Kafka Connect records to ArangoDB.
 */
public class Writer {
  public static final int UNLIMITED_BATCH_SIZE = -1;
  private enum Operation { REPSERT, DELETE }

  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private final ArangoDatabase database;
  private final String filterLatestOnField;
  private final OverwriteMode overwriteMode;
  private final int maxBatchSize;

  private static final String UDF_LATEST = "" +
      "function latest(a, b, field) {\n" +
      "  'use strict'; \n" +
      "  if (a == null) { return b; } \n" +
      "  if (a[field] > b[field]) { \n" +
      "      return a; \n" +
      "  } else { \n" +
      "      return b; \n" +
      "  } \n" +
      "}";

  /**
   * Construct a new Kafka record writer for ArangoDB.
   * @param database ArangoDB database to write to.
   */
  public Writer(final ArangoDatabase database, String condition, int maxBatchSize, String overwriteMode) {
    this.database = database;
    this.filterLatestOnField = condition;
    this.maxBatchSize = maxBatchSize;
    this.overwriteMode = OverwriteMode.valueOf(overwriteMode);
  }
  /**
   * Write Kafka records to ArangoDB.
   * @param records Records to write to ArangoDB.
   */
  public final void write(final Collection<ArangoRecord> records) {
    // Writing in batches to save trips to the database
    // Batch the records based on collection and operation in order

    // Batch
    List<ArangoRecord> batch = null;
    String batchCollection = null;
    Operation batchOperation = null;

    final Iterator<ArangoRecord> recordIterator = records.iterator();

    while (recordIterator.hasNext()) {
      // Get record information necessary for batching
      final ArangoRecord record = recordIterator.next();
      final String recordCollection = record.getCollection();
      final Operation recordOperation = this.getOperation(record);

      // Initialize the batch values for the first record to be processed
      if (batch == null) {
        batch = new ArrayList<>();
        batchCollection = recordCollection;
        batchOperation = recordOperation;
      }

      if (recordCollection.equals(batchCollection) && recordOperation == batchOperation && (maxBatchSize == UNLIMITED_BATCH_SIZE || batch.size() < maxBatchSize)) {
        // Record belongs to the batch, add it
        batch.add(record);
      } else {
        // Record does not belong to the batch, write the batch and start a new one
        this.writeBatch(batch);

        batch = new ArrayList<>();
        batch.add(record);

        batchCollection = recordCollection;
        batchOperation = recordOperation;
      }

      if (!recordIterator.hasNext()) {
        // No records remaining to add to the batch, write the batch and clean up
        this.writeBatch(batch);

        batch = null;
        batchCollection = null;
        batchOperation = null;
      }
    }
  }

  /**
   * Determine the database write operation to perform for the record.
   * @param record Record to determine the write operation for
   * @return Write operation
   */
  private Operation getOperation(final ArangoRecord record) {
    return (record.getValue() == null)
      ? Operation.DELETE
      : Operation.REPSERT;
  }

  /**
   * Write batch of ArangoRecords to the database.
   * @param batch ArangoRecords that all have the same database collection and write operation in common
   */
  private void writeBatch(final List<ArangoRecord> batch) {
    final ArangoRecord representativeRecord = batch.get(0);
    final String batchCollection = representativeRecord.getCollection();
    final Operation batchOperation = this.getOperation(representativeRecord);

    switch (batchOperation) {
      case REPSERT:
	if (this.filterLatestOnField == null || this.filterLatestOnField.isEmpty()) {
            repsertBatch(batchCollection, batch);
	} else {
	    filteredRepsertBatch(batchCollection, batch);
	}
        break;
      case DELETE:
        deleteBatch(batchCollection, batch);
        break;
      default:
        // Do nothing
    }
  }

  /**
   * Delete a batch of records from the database.
   * @param collection Name of the collection to delete from
   * @param records Records to delete
   */
  private void deleteBatch(final String collection, final List<ArangoRecord> records) {
    final List<String> documentKeys = records.stream()
        .map(record -> record.getKey())
        .collect(Collectors.toList());

    this.database.collection(collection).deleteDocuments(
        documentKeys,
        null,
        new DocumentDeleteOptions()
            .waitForSync(true)
            .silent(true));
  }

  /**
   * Repsert a batch of records to the database.
   * @param collection Name of the collection to repsert to
   * @param records Records to repsert
   */
  private void repsertBatch(final String collection, final List<ArangoRecord> records) {
    final List<String> documentValues = records.stream()
        .map(record -> record.getValue())
        .collect(Collectors.toList());

    this.database.collection(collection).insertDocuments(
        documentValues,
        new DocumentCreateOptions()
            .overwriteMode(this.overwriteMode)  // Default is "replace"
            .waitForSync(true)
            .silent(true));
  }
  /**
   * Conditional repsert a batch of records to the database.
   * @param collection Name of the collection to repsert to
   * @param records Records to repsert
   */
  private void filteredRepsertBatch(final String collection, final List<ArangoRecord> records) {
    final Object NULL_Object = new Object();
    final List<String> documentValues = records.stream()
        .map(record -> record.getValue())
	// Sanitize data
	.map((String doc) -> {
            try {
	      if (doc != null) { new JSONParser().parse(doc); }
	      return (Object)doc;
            } catch (ParseException e) {
                LOG.error("Json parse exception. collection: {}, message: {}", collection, e.getMessage());
		return NULL_Object;
	    }    
	})
        .filter(obj -> obj != NULL_Object)
	.map(obj -> (String)obj)
        .collect(Collectors.toList());

    final String query = String.format(
        "FOR doc IN [%s] UPSERT {\"_key\": doc._key } INSERT doc REPLACE CONNECTOR::UDF::LATEST(OLD, doc, \"%s\") IN \"%s\"",
        String.join(", ", documentValues),
	this.filterLatestOnField,
	collection
    );

    try { this.database.query(query, BaseDocument.class); } 
    catch (Exception e) {
        LOG.error("Upsert exception <{}>: {} Query: {}", e.getClass().getCanonicalName(), e.getMessage(), query);
        this.database.createAqlFunction("CONNECTOR::UDF::LATEST", Writer.UDF_LATEST, new AqlFunctionCreateOptions().isDeterministic(true));
        this.database.query(query, BaseDocument.class);
    }
  }
}
