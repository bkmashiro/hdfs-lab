package org.example;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HBaseStudentScoreClient {

    private static final String TABLE_NAME = "student_scores";
    private static final String COLUMN_FAMILY = "score";

    public static void main(String[] args) throws IOException {
        // Initialize HBase connection
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {

            Admin admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                System.out.println("Table does not exist. Please create the table first.");
                return;
            }

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            while (true) {
                System.out.println("HBase Java Client For Student Score:");
                System.out.println("Please select:");
                System.out.println("1. Display all grades of all students");
                System.out.println("2. View the grades of designated students");
                System.out.println("3. Insert a student's grade");
                System.out.println("4. Modify a student's grade");
                System.out.println("5. Delete a student's grade");
                System.out.println("0. Exit");
                System.out.print("> ");

                String choice = reader.readLine();
                switch (choice) {
                    case "1":
                        displayAllGrades(table);
                        break;
                    case "2":
                        viewDesignatedStudentGrades(table, reader);
                        break;
                    case "3":
                        insertStudentGrade(table, reader);
                        break;
                    case "4":
                        modifyStudentGrade(table, reader);
                        break;
                    case "5":
                        deleteStudentGrade(table, reader);
                        break;
                    case "0":
                        return;
                    default:
                        System.out.println("Invalid choice. Please select again.");
                }
            }
        }
    }

    private static void displayAllGrades(Table table) throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            System.out.println("Student: " + rowKey);
            for (Cell cell : result.rawCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(column + ": " + value);
            }
        }
        scanner.close();
    }

    private static void viewDesignatedStudentGrades(Table table, BufferedReader reader) throws IOException {
        System.out.print("Enter student name: ");
        String studentName = reader.readLine();
        Get get = new Get(Bytes.toBytes(studentName));
        Result result = table.get(get);
        if (result.isEmpty()) {
            System.out.println("No grades found for student: " + studentName);
        } else {
            System.out.println("Grades for student: " + studentName);
            for (Cell cell : result.rawCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(column + ": " + value);
            }
        }
    }

    private static void insertStudentGrade(Table table, BufferedReader reader) throws IOException {
        System.out.print("Enter student name: ");
        String studentName = reader.readLine();
        System.out.print("Enter subject: ");
        String subject = reader.readLine();
        System.out.print("Enter grade: ");
        String grade = reader.readLine();
        Put put = new Put(Bytes.toBytes(studentName));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(subject), Bytes.toBytes(grade));
        table.put(put);
        System.out.println("Grade added successfully.");
    }

    private static void modifyStudentGrade(Table table, BufferedReader reader) throws IOException {
        System.out.print("Enter student name: ");
        String studentName = reader.readLine();
        System.out.print("Enter subject: ");
        String subject = reader.readLine();
        System.out.print("Enter new grade: ");
        String grade = reader.readLine();
        Put put = new Put(Bytes.toBytes(studentName));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(subject), Bytes.toBytes(grade));
        table.put(put);
        System.out.println("Grade modified successfully.");
    }

    private static void deleteStudentGrade(Table table, BufferedReader reader) throws IOException {
        System.out.print("Enter student name: ");
        String studentName = reader.readLine();
        System.out.print("Enter subject to delete grade for: ");
        String subject = reader.readLine();
        Delete delete = new Delete(Bytes.toBytes(studentName));
        delete.addColumns(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(subject));
        table.delete(delete);
        System.out.println("Grade deleted successfully.");
    }
}
