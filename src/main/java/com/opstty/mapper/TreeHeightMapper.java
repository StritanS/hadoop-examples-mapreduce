package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightMapper extends Mapper<Object, Text, FloatWritable, NullWritable> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        try {
            String[] fields = value.toString().split(";");
            if (fields.length > 6) {
                float height = Float.parseFloat(fields[6]); // Assuming the height is the seventh field
                context.write(new FloatWritable(height), NullWritable.get());
            }
        } catch (NumberFormatException e) {
            System.err.println("NumberFormatException: " + e.getMessage() + " in input line: " + value.toString());
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println(
                    "ArrayIndexOutOfBoundsException: " + e.getMessage() + " in input line: " + value.toString());
        }
    }
}
