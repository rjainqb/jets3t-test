package com.rjain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;

import java.net.URI;

public class Jets3tTest {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.s3n.awsAccessKeyId", "<aws_access_key>");
    conf.set("fs.s3n.awsSecretAccessKey", "<aws_secret_key>");
    String file = "s3n://qubole-datasets-public/orc/526b190b4a0e4f019cf5d6cfcc1029a7_000084";
    FileSystem fs = new NativeS3FileSystem();
    Path p = new Path(file);
    fs.initialize(new URI(file), conf);
    FSDataInputStream is = fs.open(p);
    System.out.println("Seek test on file " + p);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10; ++i) {
      is.seek(i * 1000);
      System.out.println("Iteration " + i + " completed");
    }
    long end = System.currentTimeMillis();
    System.out.println("Total time: " + (end - start) / 1000.0 + " secs.");
    fs.close();
  }
}
