package com.tml.sinks;

import com.amazonaws.auth.BasicAWSCredentials;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.tml.sinks.Constants.DEFAULT_CODEC;

public class S3Sink extends AbstractSink implements Configurable, BatchSizeSupported {

  private static final Logger logger = LoggerFactory.getLogger(S3Sink.class);
  private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");
  private static String NEW_LINE = "\n";
  private static final long defaultRollInterval = 30;
  private static final int defaultBatchSize = 100;
  private static final String defaultSuffix = "";
  private static final String defaultFileName = "FlumeData";
  private static final int defaultRollTimerPoolSize = 5;
  private static final int defaultMaxOpenFiles = 5000;
  private static final int defaultFileBufferSize = 8192 ;

  private WriterLinkedHashMap sfWriters;

  private String filePath;
  private String bucketName;
  private String localBaseDirectory;
  private String fileName;
  private long rollInterval;
  private String suffix;
  private int batchSize;
  private SinkCounter sinkCounter;
  private int maxOpenFiles;
  private int rollTimerPoolSize;
  private int fileBufferSize; //in bytes
  private ExecutorService timedRollerPool;
  private String accessKey;
  private String secretKey;
  private String awsRegion;

  private ScheduledExecutorService rollService;
  private final Object sfWritersLock = new Object();
  private AWSS3Writer awss3Writer;
  private final AtomicBoolean uploadFlag = new AtomicBoolean(false);
  private final AtomicBoolean transactionRunningFlag = new AtomicBoolean(false);
  private FileCompressor fileCompressor;
  private FileNameIdentifier fileNameIdentifier;

  @Override
  public void configure(Context context) {
    bucketName = Preconditions.checkNotNull(
      context.getString("s3.bucket"), "s3.bucket is required");
    accessKey = Preconditions.checkNotNull(
      context.getString("s3.accessKey"), "s3.accessKey is required");
    secretKey = Preconditions.checkNotNull(
      context.getString("s3.secretKey"), "s3.secretKey is required");
    awsRegion = Preconditions.checkNotNull(
      context.getString("s3.awsRegion"), "s3.awsRegion is required");
    localBaseDirectory = Preconditions.checkNotNull(
      context.getString("s3.localDirectory"), "s3.localDirectory is required");
    filePath = Preconditions.checkNotNull(
      context.getString("s3.path"), "s3.path is required");
    String codeC = context.getString("s3.codeC", DEFAULT_CODEC);
    fileName = context.getString("s3.filePrefix", defaultFileName);
    rollInterval = context.getLong("s3.rollInterval", defaultRollInterval);
    suffix = context.getString("s3.fileSuffix", defaultSuffix);
    maxOpenFiles = context.getInteger("s3.maxOpenFiles", defaultMaxOpenFiles);
    rollTimerPoolSize = context.getInteger("s3.rollTimerPoolSize",
      defaultRollTimerPoolSize);
    fileBufferSize = context.getInteger("s3.fileBufferSize",
      defaultFileBufferSize);

    batchSize = context.getInteger("s3.batchSize", defaultBatchSize);

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    awss3Writer = new AWSS3Writer(awsRegion, new BasicAWSCredentials(accessKey, secretKey));
    logger.debug("Configured codeC : {}", codeC);
    fileCompressor = CompressorFactory.getInstance(codeC);
    fileNameIdentifier = new FileNameIdentifier();
  }

  @Override
  public void start() {
    logger.debug("Starting {}...", this);

    String rollerName = "s3-" + getName() + "-roll-timer-%d";
    timedRollerPool = Executors.newFixedThreadPool(rollTimerPoolSize,
      new ThreadFactoryBuilder().setNameFormat(rollerName).build());

    this.sfWriters = new WriterLinkedHashMap(maxOpenFiles);
    sinkCounter.start();

    logger.info("Rolling file before starting sink");
    FileRoller.roll(localBaseDirectory, bucketName, timedRollerPool, awss3Writer, sfWriters, uploadFlag,
      fileCompressor, fileNameIdentifier, transactionRunningFlag);

    super.start();

    if (rollInterval > 0) {

      rollService = Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder().setNameFormat(
          "rollingS3Sink-roller-" +
            Thread.currentThread().getId() + "-%d").build());

      /*
       * Every N seconds, mark that it's time to rotate. We purposefully do NOT
       * touch anything other than the indicator flag to avoid error handling
       * issues (e.g. IO exceptions occuring in two different threads.
       * Resist the urge to actually perform rotation in a separate thread!
       */
      rollService.scheduleAtFixedRate(() -> {
        logger.debug("Started rolling the file");
          FileRoller.roll(localBaseDirectory, bucketName, timedRollerPool, awss3Writer, sfWriters, uploadFlag,
            fileCompressor, fileNameIdentifier, transactionRunningFlag);
      }, rollInterval, rollInterval, TimeUnit.SECONDS);

    } else {
      logger.debug("RollInterval is not valid, file rolling will not happen.");
    }
    logger.debug("S3Sink {} started.", getName());
  }

  private String getDirectoryPath(String filePath, Map<String, String> headers) {
    return BucketPath.escapeString(filePath, headers);
  }

  @Override
  public Status process() throws EventDeliveryException {
    if(uploadFlag.get()) {
      logger.debug("Upload in progress");
      return Status.BACKOFF;
    }
    transactionRunningFlag.set(true);
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    try {
      int txnEventCount = 0;
      //ExecutorService fixedThreadPool = Executors.newFixedThreadPool(10);
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();
        if (event == null) {
          break;
        }

        // reconstruct the path name by substituting place holders
        String realPath = getDirectoryPath(filePath, event.getHeaders());
        String realName = BucketPath.escapeString(fileName, event.getHeaders());
        String realSuffix = BucketPath.escapeString(suffix, event.getHeaders());

        String lookupPath = localBaseDirectory + realPath + DIRECTORY_DELIMITER + realName + "-" +
          fileNameIdentifier.getIdentifier() + realSuffix;
        BufferedOutputStream outputStream = sfWriters.get(lookupPath);
        // we haven't seen this file yet, so open it and cache the handle
        if (outputStream == null) {
          File file = new File(lookupPath);
          if (!file.exists()) {
            logger.debug("Creating {}", lookupPath);
            logger.debug("START new file generation {}", LocalDateTime.now());
            file.getParentFile().mkdirs();
            file.createNewFile();
            logger.debug("STOP new file generation {}", LocalDateTime.now());
          } else {
            logger.debug("File already exists : {}", lookupPath);
          }

          outputStream = new BufferedOutputStream(new FileOutputStream(lookupPath, true), fileBufferSize);

          logger.debug("START update sfWriters {}", LocalDateTime.now());
          sfWriters.put(lookupPath, outputStream);
          logger.debug("STOP update sfWriters {}", LocalDateTime.now());
        } else {
          logger.debug("Output stream already exists : {}", lookupPath);
        }

        logger.debug("START add message to file {}", LocalDateTime.now());
        /*BufferedOutputStream finalOutputStream = outputStream;
        fixedThreadPool.submit(() -> {
          try {
            synchronized (finalOutputStream) {
              finalOutputStream.write(event.getBody());
              finalOutputStream.write(NEW_LINE.getBytes());
            }
          } catch (IOException e) {
            logger.error("file write fail", e);
          }
        });*/

        outputStream.write(event.getBody(), 0, event.getBody().length);
        outputStream.write(NEW_LINE.getBytes(), 0, NEW_LINE.getBytes().length);

        logger.debug("STOP add message to file {}", LocalDateTime.now());

        if (txnEventCount == 0) {
          sinkCounter.incrementBatchEmptyCount();
        } else if (txnEventCount == batchSize) {
          sinkCounter.incrementBatchCompleteCount();
        } else {
          sinkCounter.incrementBatchUnderflowCount();
        }
      }

      /*fixedThreadPool.shutdown();
      fixedThreadPool.awaitTermination(1, TimeUnit.MINUTES);*/

      /*logger.debug("START flush {}", LocalDateTime.now());
      for (OutputStream outputStream : outputStreams) {
        outputStream.flush();
      }
      logger.debug("STOP flush {}", LocalDateTime.now());*/

      logger.debug("START transaction commit {}", LocalDateTime.now());
      transaction.commit();
      logger.debug("STOP transaction commit {}", LocalDateTime.now());

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        return Status.READY;
      }

    } catch (IOException eIO) {
      transaction.rollback();
      logger.warn("S3 IO error", eIO);
      sinkCounter.incrementEventWriteFail();
      return Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      logger.error("process failed", th);
      sinkCounter.incrementEventWriteOrChannelFail(th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      transaction.close();
      transactionRunningFlag.set(false);
    }
  }

  @Override
  public long getBatchSize() {
    return 0;
  }
}
