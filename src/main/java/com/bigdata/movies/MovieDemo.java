package com.bigdata.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class MovieDemo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MRAnalysis.class);

        job.setMapperClass(MRAnalysis.MapMovies.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        String inPath = "datas/douban.txt";
        String inPath = "file:///E:\\idea_projects\\MovieAnalysisProject\\datas\\douban.txt";
//        Path outPath = new Path("./outD001");
//        读取本地数据，将其写入hadoop集群指定路径
        Path outPath = new Path("hdfs://hd01:9000/douban/output001");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, outPath);

        boolean completion = job.waitForCompletion(true);
        System.out.println(completion);
        System.out.println("git test");
    }
}
