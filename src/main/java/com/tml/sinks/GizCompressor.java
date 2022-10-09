package com.tml.sinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPOutputStream;

class GzipCompressor implements FileCompressor {

  private static final Logger logger = LoggerFactory.getLogger(GzipCompressor.class);

  GzipCompressor() {
    logger.info("Configured GizCompressor..");
  }

  @Override
  public File compress(File uncompressedFile) {

    String compressedFilePath = uncompressedFile.getAbsoluteFile() + ".gz";

    try (GZIPOutputStream gos = new GZIPOutputStream(
      new FileOutputStream(compressedFilePath));
         FileInputStream fis = new FileInputStream(uncompressedFile)) {

      byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        gos.write(buffer, 0, len);
      }

      return new File(compressedFilePath);
    } catch (IOException e) {
      logger.error("Error while compressing file", e);
    }
    return null;
  }
}
