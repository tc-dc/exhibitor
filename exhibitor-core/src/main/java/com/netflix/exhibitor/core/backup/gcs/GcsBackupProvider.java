package com.netflix.exhibitor.core.backup.gcs;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.netflix.exhibitor.core.backup.s3.S3BackupProvider;
import com.netflix.exhibitor.core.backup.s3.Throttle;
import com.netflix.exhibitor.core.s3.S3ClientFactory;
import com.netflix.exhibitor.core.s3.S3Credential;

import org.apache.curator.RetryPolicy;
import org.apache.curator.utils.CloseableUtils;

public class GcsBackupProvider extends S3BackupProvider {
  public GcsBackupProvider(S3ClientFactory factory) throws Exception {
    super(factory, (S3Credential)null, null);
  }

  @Override
  protected void multiPartUpload(File source, Map<String, String> configValues, RetryPolicy retryPolicy, Throttle throttle, String key) throws Exception {
    FileInputStream in = new FileInputStream(source);
    try {
      String bucket = configValues.get(CONFIG_BUCKET.getKey());
      ObjectMetadata md = new ObjectMetadata();
      PutObjectRequest por = new PutObjectRequest(bucket, key, in, md);
      getS3Client().putObject(por);
    }
    finally {
      CloseableUtils.closeQuietly(in);
    }
  }
}
