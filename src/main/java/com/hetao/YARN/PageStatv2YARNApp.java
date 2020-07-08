package com.hetao.YARN;

import com.hetao.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageStatv2YARNApp {

    public static void main(String[] args) throws Exception{

        Configuration configuration=new Configuration();

        FileSystem fileSystem=FileSystem.get(configuration);
        Path outputPath=new Path(args[1]);
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }

        Job job=Job.getInstance(configuration);

        job.setJarByClass(PageStatv2YARNApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,outputPath);


        job.waitForCompletion(true);
    }

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

            Map<String,String> info=logParser.parsev2(log);   //获取到信息
            context.write(new Text(info.get("pageId")),ONE);

        }
    }


    static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for (LongWritable value:values){
                count++;
            }
            context.write(key,new LongWritable(count));
        }
    }
}
