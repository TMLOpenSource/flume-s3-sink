package com.tml.sinks;

import org.apache.commons.lang.StringUtils;

import static com.tml.sinks.Constants.DEFAULT_CODEC;

final class CompressorFactory {

  static FileCompressor getInstance(String codeC) {
    if(StringUtils.isBlank(codeC) || StringUtils.endsWithIgnoreCase(DEFAULT_CODEC, codeC)) {
      return new NullCompressor();
    }
    if(StringUtils.endsWithIgnoreCase(codeC, "gz")) {
      return new GizCompressor();
    }
    return new NullCompressor();
  }
}
