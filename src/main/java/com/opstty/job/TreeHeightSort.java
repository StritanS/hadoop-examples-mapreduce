package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.opstty.mapper.TreeHeightMapper;
import com.opstty.reducer.TreeHeightReducer;

public class TreeHeightSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: treeheightsort <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sort tree heights");
        job.setJarByClass(TreeHeightSort.class);
        job.setMapperClass(TreeHeightMapper.class);
        job.setCombinerClass(TreeHeightReducer.class);
        job.setReducerClass(TreeHeightReducer.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}