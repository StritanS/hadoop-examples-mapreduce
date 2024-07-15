package com.opstty.mapper;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeAgeMapper extends Mapper<Object, Text, NullWritable, DistrictAgeWritable> {
    private boolean isHeader = true;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return; // Skip the header row
        }
        try {
            String[] fields = value.toString().split(";");
            if (fields.length > 5) {
                String district = fields[1]; // Assuming the district is the second field
                int age = 2023 - Integer.parseInt(fields[5]); // Assuming the year is the sixth field
                context.write(NullWritable.get(), new DistrictAgeWritable(district, age));
            }
        } catch (NumberFormatException e) {
            System.err.println("NumberFormatException: " + e.getMessage() + " in input line: " + value.toString());
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println(
                    "ArrayIndexOutOfBoundsException: " + e.getMessage() + " in input line: " + value.toString());
        }
    }
}
