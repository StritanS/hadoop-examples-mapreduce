package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeKindHeightMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        try {
            String[] fields = value.toString().split(";");
            if (fields.length > 6) {
                String kind = fields[3]; // Assuming the kind is the fourth field
                float height = Float.parseFloat(fields[6]); // Assuming the height is the seventh field
                context.write(new Text(kind), new FloatWritable(height));
            }
        } catch (NumberFormatException e) {
            System.err.println("NumberFormatException: " + e.getMessage() + " in input line: " + value.toString());
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println(
                    "ArrayIndexOutOfBoundsException: " + e.getMessage() + " in input line: " + value.toString());
        }
    }
}
