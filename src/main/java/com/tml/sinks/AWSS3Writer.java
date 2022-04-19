package com.tml.sinks;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class AWSS3Writer {

  private AmazonS3 s3Client;
  private static final Logger logger = LoggerFactory.getLogger(AWSS3Writer.class);

  AWSS3Writer(String clientRegion, AWSCredentials credentials, String vpcEndpointId) {

    try {
      AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
        .withRegion(clientRegion)
        .withCredentials(new AWSStaticCredentialsProvider(credentials));

      if (vpcEndpointId != null) {
        amazonS3ClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(vpcEndpointId, clientRegion));
      }
      s3Client = amazonS3ClientBuilder.build();
    } catch (Exception e) {
      logger.error("Error while creating S3 Client", e);
      throw e;
    }
  }

  void upload(String bucketName, String baseDirectoryPath, File file) {
    logger.info("Uploading.. {}", file.getAbsolutePath());
    s3Client.putObject(bucketName, file.getAbsolutePath().replace(baseDirectoryPath, ""), file);
  }

}
