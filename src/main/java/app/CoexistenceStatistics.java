package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Task2：特征抽取，人物同现统计
 */

public class CoexistenceStatistics {
    public static class CoexistenceStatisticsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text K = new Text();
        private IntWritable V = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 分割段落，得到出现的character数组
            String[] characters = value.toString().split("\t");
            // 数组去重，得到character集合
            Set<String> characterSet = new HashSet<String>(Arrays.asList(characters));
            // 遍历实现该段落的同现统计
            for (String characterA : characterSet) {
                for (String characterB : characterSet) {
                    if (!characterA.equals(characterB)) {
                        K.set("<" + characterA + "," + characterB + ">");
                        context.write(K, V);
                    }
                }
            }
        }
    }

    public static class CoexistenceStatisticsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable V = new IntWritable(0);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            V.set(sum);
            context.write(key, V);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //以下配置均参考自官方文档
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task2:CharacterCoexistenceStatistics");
        job.setJarByClass(CoexistenceStatistics.class);
        job.setMapperClass(CoexistenceStatistics.CoexistenceStatisticsMapper.class);
        job.setReducerClass(CoexistenceStatistics.CoexistenceStatisticsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
