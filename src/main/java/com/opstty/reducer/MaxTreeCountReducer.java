package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeCountReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    private Text maxDistrict = new Text();
    private int maxCount = 0;

    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            String district = parts[0];
            int count = Integer.parseInt(parts[1]);
            if (count > maxCount) {
                maxCount = count;
                maxDistrict.set(district);
            }
        }
        context.write(maxDistrict, new IntWritable(maxCount));
    }
}
