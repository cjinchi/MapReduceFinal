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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

public class TaskSeven {
    public static class TaskSevenMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text name =new Text();
            Text label = new Text();
            if (itr.hasMoreTokens())
                name.set(itr.nextToken());
            if (itr.hasMoreTokens())
                label.set(itr.nextToken());
            context.write(label,name);

        }
    }

    public static class TaskSevenReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }
        }
    }
/*
    public static class TaskSevenPartitioner extends HashPartitioner<Text, IntWritable> {
        private Text term = new Text();

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            term.set(value.toString().split(",")[0]);
            return super.getPartition(term, value, numReduceTasks);
        }
    }
*/
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //以下配置均参考自官方文档
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task7");
        job.setJarByClass(TaskSeven.class);
        job.setMapperClass(TaskSeven.TaskSevenMapper.class);
        //      job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
        job.setReducerClass(TaskSeven.TaskSevenReducer.class);
   //     job.setPartitionerClass(TaskSeven.TaskSevenPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
