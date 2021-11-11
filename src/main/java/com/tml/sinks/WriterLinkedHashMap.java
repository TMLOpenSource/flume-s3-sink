package com.tml.sinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/*
 * Extended Java LinkedHashMap for open file handle LRU queue.
 * We want to clear the oldest file handle if there are too many open ones.
 */
public class WriterLinkedHashMap extends LinkedHashMap<String, BufferedOutputStream> {

  private static final Logger logger = LoggerFactory.getLogger(WriterLinkedHashMap.class);
  private final int maxOpenFiles;

  public WriterLinkedHashMap(int maxOpenFiles) {
    super(16, 0.75f, true); // stock initial capacity/load, access ordering
    this.maxOpenFiles = maxOpenFiles;
  }

  public void close() {
    this.entrySet().stream().forEach(entry -> {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        logger.error(entry.getKey(), e);
        Thread.currentThread().interrupt();
      }
    });
    clear();
  }

  @Override
  protected boolean removeEldestEntry(Entry<String, BufferedOutputStream> eldest) {
    if (size() > maxOpenFiles) {
      // If we have more that max open files, then close the last one and
      // return true
      BufferedOutputStream outputStream = eldest.getValue();
      try {
        outputStream.close();
      } catch (IOException e) {
        logger.warn(eldest.getKey(), e);
        Thread.currentThread().interrupt();
      }
      return true;
    } else {
      return false;
    }
  }
}
