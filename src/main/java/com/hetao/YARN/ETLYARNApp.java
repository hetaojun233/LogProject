package com.hetao.YARN;

import com.hetao.utils.ContentUtils;
import com.hetao.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETLYARNApp {

    public static void main(String[] args) throws Exception{

        Configuration configuration=new Configuration();

        FileSystem fileSystem=FileSystem.get(configuration);
        Path outputPath=new Path(args[1]);
        if (fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }

        Job job=Job.getInstance(configuration);

        job.setJarByClass(ETLYARNApp.class);

        job.setMapperClass(MyMapper.class);


        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,outputPath);


        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable, Text,NullWritable,Text> {


        private LongWritable ONE=new LongWritable(1);

        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser=new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log =value.toString();

            Map<String,String> info=logParser.parse(log);   //获取到信息

            String ip=info.get("ip");
            String country=info.get("country");
            String province=info.get("province");
            String city=info.get("city");
            String url=info.get("url");
            String time=info.get("time");
            String pageId=ContentUtils.getPageId(url)==""?"-":ContentUtils.getPageId(url);


            StringBuilder builder=new StringBuilder();
            builder.append(ip).append("\t");
            builder.append(country).append("\t");
            builder.append(province).append("\t");
            builder.append(city).append("\t");
            builder.append(url).append("\t");
            builder.append(time).append("\t");
            builder.append(pageId);

            context.write(NullWritable.get(),new Text(builder.toString()));
            }
        }
    }

