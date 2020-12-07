package com.bigdata.movies;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MRAnalysis {
    public static String showingDateFormat(String dateTime) {
        String dateTimes = dateTime.replaceAll("/", "-");
        Pattern pattern = Pattern.compile(".*?([0-9]{4}-[0-9]{1,2}-[0-9]{1,2}).*?");
        Matcher matcher = pattern.matcher(dateTimes);
//        System.out.println(matcher.find());
        if (matcher.find()) {
            //            System.out.println(group);
            return matcher.group(1);
        }
        return "";
    }

    public static class MapMovies extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            根据字段分隔符对 每行数据进行分割
            String[] movieInfos = value.toString().trim().split("\t");

//            查看，发现key 偏移量是从0 开始的，所以我们可以用它来判断是否是第一行数据
//            System.out.println(key.get());
            if (key.get() != 0
                    && movieInfos.length == 6
                    && !movieInfos[1].contains("评价人数不足")
                    && !movieInfos[1].contains("目前无")
                    && !movieInfos[1].contains("尚未上映")
                    && !movieInfos[1].trim().equals("")
            ) {
                String showingTime = showingDateFormat(movieInfos[4]);
                if (!showingTime.equals("")){
                    String movieName = StringUtils.strip(movieInfos[0], " ");
//                获取 评价中的人数
                    String comment = StringUtils.strip(movieInfos[2], "人评价)(");
                    String actors = StringUtils.strip(movieInfos[5], "[ ]'");

                    text.set(movieName+"\t"+movieInfos[1]+"\t"+comment+"\t"+movieInfos[3]+"\t"+showingTime+"\t"+actors);
                    context.write(text, NullWritable.get());
                }

            }
        }
    }
}
