package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IPCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text ip = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String ipAddress = value.toString();
        ip.set(ipAddress);
        context.write(ip, one);
    }
}