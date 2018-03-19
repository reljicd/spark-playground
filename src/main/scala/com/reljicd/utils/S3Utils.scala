package com.reljicd.utils

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.{GetObjectRequest, PutObjectRequest}
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}

trait S3Utils {

  def S3FileToLocalFile(s3FilePath: String, localDirPath: String): File = {
    val amazonS3Client = getAmazonS3Client

    val amazonS3URI = new AmazonS3URI(s3FilePath)

    val localFile = new File(localDirPath + "/" + getFileNameFromS3Url(s3FilePath))
    amazonS3Client.getObject(new GetObjectRequest(amazonS3URI.getBucket, amazonS3URI.getKey), localFile)
    localFile
  }

  def uploadFileToS3(file: File, s3FilePath: String): Unit = {
    val amazonS3Client = getAmazonS3Client

    val amazonS3URI = new AmazonS3URI(s3FilePath)

    amazonS3Client.putObject(new PutObjectRequest(amazonS3URI.getBucket, amazonS3URI.getKey, file))
  }

  def uploadDirToS3(directory: File, s3FilePath: String): Unit = {
    val amazonS3Client = getAmazonS3Client

    val amazonS3URI = new AmazonS3URI(s3FilePath)

    val tx = new TransferManager(amazonS3Client)
    val myUpload = tx.uploadDirectory(amazonS3URI.getBucket, amazonS3URI.getKey, directory, true)
    myUpload.waitForCompletion()
    tx.shutdownNow()
  }

  def getAmazonS3Client: AmazonS3Client = {
    val yourAWSCredentials = new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"))
    new AmazonS3Client(yourAWSCredentials)
  }

  def getFileNameFromS3Url(s3url: String): String = {
    s3url.replaceFirst(".*/([^/?]+).*", "$1")
  }
}
