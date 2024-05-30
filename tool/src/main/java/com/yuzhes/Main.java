package com.yuzhes;

import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        HDFSClient client;
        try {
            client = new HDFSClient();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Scanner scn = new Scanner(System.in);
        while (true) {
            System.out.println("HDFS Java Client");
            System.out.println("Please select:");
            System.out.println("1. List all files in a specific directory");
            System.out.println("2. Upload specific files to the specified directory");
            System.out.println("3. Delete files in specific directories");
            System.out.println("4. Display specific file details");
            System.out.println("0. Exit");
            System.out.print("> ");

            try {
                int choice = Integer.parseInt(scn.nextLine());
                switch (choice) {
                    case 1:
                        System.out.print("Enter the HDFS directory path: ");
                        String dirPath = scn.nextLine().trim();
                        client.listFiles(dirPath);
                        break;
                    case 2:
                        System.out.print("Enter the local file path: ");
                        String localFilePath = scn.nextLine().trim();
                        System.out.print("Enter the HDFS directory path: ");
                        String hdfsDirPath = scn.nextLine().trim();
                        client.uploadFile(localFilePath, hdfsDirPath);
                        break;
                    case 3:
                        System.out.print("Enter the HDFS file path to delete: ");
                        client.deleteFile(scn.nextLine().trim());
                        break;
                    case 4:
                        System.out.print("Enter the HDFS file path: ");
                        client.displayFileDetails(scn.nextLine().trim());
                        break;
                    case 0:
                        client.close();
                        System.exit(0);
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                scn.close();
            }
        }
    }
}