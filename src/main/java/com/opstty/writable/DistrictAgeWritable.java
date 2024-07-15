package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistrictAgeWritable implements Writable {
    private Text district;
    private IntWritable age;

    public DistrictAgeWritable() {
        this.district = new Text();
        this.age = new IntWritable();
    }

    public DistrictAgeWritable(String district, int age) {
        this.district = new Text(district);
        this.age = new IntWritable(age);
    }

    public Text getDistrict() {
        return district;
    }

    public IntWritable getAge() {
        return age;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        district.write(out);
        age.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        district.readFields(in);
        age.readFields(in);
    }

    @Override
    public String toString() {
        return district.toString() + "\t" + age.toString();
    }
}
