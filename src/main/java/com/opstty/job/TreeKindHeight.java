package com.opstty.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.opstty.mapper.TreeKindHeightMapper;
import com.opstty.reducer.TreeKindHeightReducer;

public class TreeKindHeight {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: treekindheight <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "max height of tree kinds");
        job.setJarByClass(TreeKindHeight.class);
        job.setMapperClass(TreeKindHeightMapper.class);
        job.setCombinerClass(TreeKindHeightReducer.class);
        job.setReducerClass(TreeKindHeightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
