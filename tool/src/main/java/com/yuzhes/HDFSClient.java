package com.yuzhes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public class HDFSClient {
    private final FileSystem fs;

    public HDFSClient() throws IOException {
        Configuration conf = new Configuration();
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        fs = FileSystem.get(conf);
    }

    public void listFiles(String dirPath) throws IOException {
        Path path = new Path(dirPath);

        if (!fs.exists(path)) {
            System.out.println("Directory does not exist.");
            return;
        }

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, false);
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            System.out.println(fileStatus.getPath().toString());
        }
    }

    public void uploadFile(String localFilePath, String hdfsDirPath) throws IOException {
        Path localPath = new Path(localFilePath);
        Path hdfsPath = new Path(hdfsDirPath);

        if (!fs.exists(hdfsPath)) {
            System.out.println("HDFS directory does not exist.");
            return;
        }

        fs.copyFromLocalFile(localPath, hdfsPath);
        System.out.println("File uploaded successfully.");
    }

    public void deleteFile(String filePath) throws IOException {

        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            System.out.println("File does not exist.");
            return;
        }

        fs.delete(path, true);
        System.out.println("File deleted successfully.");
    }

    public void displayFileDetails(String filePath) throws IOException {
        Path path = new Path(filePath);

        if (!fs.exists(path)) {
            System.out.println("File does not exist.");
            return;
        }

        FileStatus fileStatus = fs.getFileStatus(path);
        System.out.println("Path: " + fileStatus.getPath());
        System.out.println("Length: " + fileStatus.getLen());
        System.out.println("Owner: " + fileStatus.getOwner());
        System.out.println("Group: " + fileStatus.getGroup());
        System.out.println("Permission: " + fileStatus.getPermission());
        System.out.println("Modification Time: " + fileStatus.getModificationTime());
    }

    public void close() throws IOException {
        fs.close();
    }
}