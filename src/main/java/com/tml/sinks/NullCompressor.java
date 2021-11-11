package com.tml.sinks;

import java.io.File;

public class NullCompressor implements FileCompressor {

  @Override
  public File compress(File uncompressedFile) {
    return uncompressedFile;
  }
}
