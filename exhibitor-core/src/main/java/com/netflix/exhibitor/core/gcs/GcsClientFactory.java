package com.netflix.exhibitor.core.gcs;

import com.netflix.exhibitor.core.s3.S3Client;
import com.netflix.exhibitor.core.s3.S3ClientConfig;
import com.netflix.exhibitor.core.s3.S3ClientFactory;
import com.netflix.exhibitor.core.s3.S3Credential;
import com.netflix.exhibitor.core.s3.S3CredentialsProvider;

public class GcsClientFactory implements S3ClientFactory {
  @Override
  public S3Client makeNewClient(S3Credential credentials, String s3Region) throws Exception {
    return new GcsClient();
  }

  @Override
  public S3Client makeNewClient(S3CredentialsProvider credentialsProvider, String s3Region) throws Exception {
    return new GcsClient();
  }

  @Override
  public S3Client makeNewClient(S3Credential credentials, S3ClientConfig clientConfig, String s3Region) throws Exception {
    return new GcsClient();
  }

  @Override
  public S3Client makeNewClient(S3CredentialsProvider credentialsProvider, S3ClientConfig clientConfig, String s3Region) throws Exception {
    return new GcsClient();
  }
}
