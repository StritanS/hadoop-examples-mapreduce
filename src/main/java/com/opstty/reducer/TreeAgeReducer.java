package com.opstty.reducer;

import com.opstty.writable.DistrictAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TreeAgeReducer extends Reducer<NullWritable, DistrictAgeWritable, Text, IntWritable> {
    private DistrictAgeWritable oldestTree = new DistrictAgeWritable("", Integer.MAX_VALUE);

    public void reduce(NullWritable key, Iterable<DistrictAgeWritable> values, Context context)
            throws IOException, InterruptedException {
        for (DistrictAgeWritable val : values) {
            if (val.getAge().get() < oldestTree.getAge().get()) {
                oldestTree = val;
            }
        }
        context.write(oldestTree.getDistrict(), oldestTree.getAge());
    }
}
