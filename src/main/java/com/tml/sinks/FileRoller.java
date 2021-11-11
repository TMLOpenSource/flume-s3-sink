package com.tml.sinks;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

class FileRoller {

  private static final Logger logger = LoggerFactory.getLogger(S3Sink.class);
  private static final String compressedFileExtension="gz";

  public static void roll(final String localBaseDirectory, final String bucketName,
                          final ExecutorService timedRollerPool,
                          final AWSS3Writer awss3Writer,
                          final WriterLinkedHashMap sfWriters,
                          final AtomicBoolean uploadFlag,
                          final FileCompressor fileCompressor,
                          final FileNameIdentifier fileNameIdentifier,
                          final AtomicBoolean transactionRunningFlag) {
    Collection<File> filesToUpload = FileUtils.listFiles(new File(localBaseDirectory), null, true);
    if(getFilteredFileStream(filesToUpload).count()==0) {
      logger.info("Nothing to upload..");
      return;
    }

    if(uploadFlag.get()) {
      logger.info("Already uploading..");
      return;
    }

    uploadFlag.set(true);
    while(transactionRunningFlag.get()); //wait to complete current sink transaction

    final long countToBeUploaded = getFilteredFileStream(filesToUpload).count();
    AtomicLong count = new AtomicLong();
    sfWriters.close();
    logger.info("Starting upload");
    getFilteredFileStream(filesToUpload)
      .forEach(file -> timedRollerPool.submit(() -> {
        try {
          File compressedFile=file;
          String extension = FilenameUtils.getExtension(file.getName());
          if (!extension.equals(compressedFileExtension))
            compressedFile = fileCompressor.compress(file);
          awss3Writer.upload(bucketName, localBaseDirectory, compressedFile);
          file.delete();
          if(compressedFile.exists()) compressedFile.delete();
        } finally {
          count.getAndIncrement();
          if(count.get() == countToBeUploaded) {
            logger.info("Releasing upload lock");
            //writeLock.unlock();
            fileNameIdentifier.generateNewIdentifier();
            uploadFlag.set(false);
          }
        }
      }));
  }

  private static Stream<File> getFilteredFileStream(Collection<File> filesToUpload) {
    return filesToUpload.stream()
      .filter(file -> !file.isHidden());
  }
}