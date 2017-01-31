package com.netflix.exhibitor.core.gcs;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.util.Base64;
import com.google.cloud.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.netflix.exhibitor.core.s3.S3Client;
import com.netflix.exhibitor.core.s3.S3ClientConfig;
import com.netflix.exhibitor.core.s3.S3Credential;
import com.netflix.exhibitor.core.s3.S3Utils;

public class GcsClient implements S3Client {
  private Storage gceStorage;

  public GcsClient() {
    gceStorage = StorageOptions.getDefaultInstance().getService();
  }

  @Override
  public void changeCredentials(S3Credential credential) throws Exception {
  }

  @Override
  public void changeCredentials(S3Credential credential, S3ClientConfig clientConfig) throws Exception {
  }

  private Exception notFound() throws Exception {
    AmazonS3Exception ex = new AmazonS3Exception("not found");
    ex.setStatusCode(404);
    throw ex;
  }

  @Override
  public S3Object getObject(String bucket, String key) throws Exception {
    Blob b = gceStorage.get(bucket, key);
    if (b == null) {
      throw notFound();
    }
    return new GcsObject(b);
  }

  @Override
  public ObjectMetadata getObjectMetadata(String bucket, String key) throws Exception {
    Blob b = gceStorage.get(bucket, key);
    if (b == null) {
      throw notFound();
    }
    return new GcsObject(b).getObjectMetadata();
  }

  @Override
  public ObjectListing listObjects(ListObjectsRequest request) throws Exception {
    Page<Blob> result = gceStorage.list(
        request.getBucketName(),
        Storage.BlobListOption.prefix(request.getPrefix()));

    ObjectListing listing = new ObjectListing();
    listing.setTruncated(false);
    listing.setBucketName(request.getBucketName());
    List<S3ObjectSummary> summaries = listing.getObjectSummaries();

    Iterator<Blob> iter = result.iterateAll();
    while(iter.hasNext()) {
      Blob b = iter.next();
      S3ObjectSummary os = new S3ObjectSummary();
      os.setKey(b.getName());
      os.setBucketName(b.getBucket());
      summaries.add(os);
    }
    return listing;
  }

  @Override
  public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws Exception {
    throw new Exception("listNextBatchOfObjects is unsupported");
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest request) throws Exception {
    BlobInfo.Builder bi = BlobInfo
        .newBuilder(request.getBucketName(), request.getKey());

    String md5 = request.getMetadata().getContentMD5();
    if (md5 != null) {
      bi.setMd5(md5);
    }

    Blob result = gceStorage.create(bi.build(), request.getInputStream());
    PutObjectResult por = new PutObjectResult();
    por.setContentMd5(md5);
    por.setETag(md5);
    byte[] rawMd5 = Base64.decode(result.getMd5());
    por.setETag(S3Utils.toHex(rawMd5));
    return por;
  }

  @Override
  public void deleteObject(String bucket, String key) throws Exception {
    gceStorage.delete(bucket, key);
  }


  @Override
  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws Exception {
    throw new Exception("not supported");
  }

  @Override
  public UploadPartResult uploadPart(UploadPartRequest request) throws Exception {
    throw new Exception("not supported");
  }

  @Override
  public void completeMultipartUpload(CompleteMultipartUploadRequest request) throws Exception {
    throw new Exception("not supported");
  }

  @Override
  public void abortMultipartUpload(AbortMultipartUploadRequest request) throws Exception {
    throw new Exception("not supported");
  }

  @Override
  public void close() throws IOException {

  }
}
