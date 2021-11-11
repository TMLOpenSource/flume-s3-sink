package com.tml.sinks;

import java.io.File;

interface FileCompressor {

  File compress(File uncompressedFile);
}
