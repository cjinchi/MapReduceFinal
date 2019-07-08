package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

public class TaskSix {
    public static class TaskSixMapper extends Mapper<Object, Text, Text,Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text name = new Text();
            Text pagerank = new Text();
            if (itr.hasMoreTokens())
                name.set(itr.nextToken());
            if (itr.hasMoreTokens())
                pagerank.set(itr.nextToken());
            context.write(pagerank,name);
        }

    }

    public static class TaskSixReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task6-1");
        job.setJarByClass(TaskSix.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //设置比较器，用于比较数据的大小，然后按顺序排序，该例子主要用于比较两个key的大小



        //设置保存partitions文件的路径
        Path partitionFile = new Path(new Path(args[0]), "_partitions");
        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

        //key值采样，0.01是采样率，
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);


        job.setMapperClass(TaskSix.TaskSixMapper.class);
        //      job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
        job.setReducerClass(TaskSix.TaskSixReducer.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
