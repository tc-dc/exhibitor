package com.netflix.exhibitor.core.gcs;

import java.nio.channels.Channels;
import java.util.Date;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;

public class GcsObject extends S3Object {
  private Blob src;
  GcsObject(Blob src) {
    this.src = src;
    this.setBucketName(src.getBucket());
    this.setKey(src.getName());

    ObjectMetadata md = new ObjectMetadata();
    md.setUserMetadata(src.getMetadata());
    md.setContentLength(src.getSize());
    md.setLastModified(new Date(src.getUpdateTime()));
    this.setObjectMetadata(md);
  }

  @Override
  public S3ObjectInputStream getObjectContent() {
    ReadChannel reader = src.reader();
    return new S3ObjectInputStream(Channels.newInputStream(reader), null);
  }
}
