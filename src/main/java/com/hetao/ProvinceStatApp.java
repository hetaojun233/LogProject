package com.hetao;

import com.hetao.utils.LogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProvinceStatApp {


    static class MyMapper extends Mapper<LongWritable,Text, Text,LongWritable>{


        private LongWritable ONE=new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser=new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log =value.toString();
            
        }
    }
}
