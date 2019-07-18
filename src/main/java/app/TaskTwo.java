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

public class TaskTwo {
    public static class TaskTwoMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 分割段落，得到出现的character数组
            String[] roles = value.toString().trim().split(" ");
            // 数组去重，得到character集合
            Set<String> roleSet = new HashSet<>(Arrays.asList(roles));
            // 遍历实现该段落的同现统计
            for (String roleOne : roleSet) {
                for (String roleTwo : roleSet) {
                    if (!roleOne.equals(roleTwo)) {
                        context.write(new Text(String.format("<%s,%s>",roleOne,roleTwo)), ONE);
                    }
                }
            }
        }
    }

    public static class TaskTwoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task Two");
        job.setJarByClass(TaskTwo.class);
        job.setMapperClass(TaskTwoMapper.class);
        job.setReducerClass(TaskTwoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
