package io.github.jaredpetersen.kafkaconnectarangodb.sink.errors;

import java.net.MalformedURLException;

class ExternalMessageDataMalformedURLException extends MalformedURLException {
  public ExternalMessageDataMalformedURLException(final MalformedURLException e) {
    super("External Message Data: Malformed URL", e);
  }
} 
