package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class TaskSixOne {

    public static class TaskSixOneStepOneMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            context.write(new DoubleWritable(Double.valueOf(items[1])), new Text(items[0]));
        }
    }

    public static class TaskSixOneStepOneReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }


    public static class TaskSixOneStepTwoMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class TaskSixOneStepTwoReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new DoubleWritable(Double.valueOf(key.toString())));
            }
        }
    }

//    public static class KeyComparator extends WritableComparator {
//        protected KeyComparator(){
//            super(Text.class,true);
//        }
//        @Override
//        public int compare(WritableComparable writableComparable1,WritableComparable writableComparable2){
//            double value1=Double.valueOf(writableComparable1.toString());
//            double value2=Double.valueOf(writableComparable2.toString());
//            return  (int)(value1-value2);
//        }
//    }

    public static class MyComparator extends WritableComparator {
        protected MyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
//            throw new RuntimeException(a.toString() + "|" + b.toString());
            return -Double.compare(Double.valueOf(a.toString()), Double.valueOf(b.toString()));
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 4) {
            throw new RuntimeException();
        }
        // Args: [input] [output] [partition] [inter]

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Task Six Job One Step One");
        job1.setJarByClass(TaskSixOne.class);
        job1.setMapperClass(TaskSixOneStepOneMapper.class);
        job1.setReducerClass(TaskSixOneStepOneReducer.class);
        job1.setMapOutputKeyClass(DoubleWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(DoubleWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));
        job1.setNumReduceTasks(5);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.totalorderpartitioner.naturalorder", "false");
        Job job2 = Job.getInstance(conf2, "Task Six Job One Step Two");
        job2.setJarByClass(TaskSixOne.class);
        job2.setMapperClass(TaskSixOneStepTwoMapper.class);
        job2.setReducerClass(TaskSixOneStepTwoReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setSortComparatorClass(MyComparator.class);
        job2.setNumReduceTasks(5);
        TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(), new Path(args[2]));
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
        InputSampler.writePartitionFile(job2, sampler);
        job2.setPartitionerClass(TotalOrderPartitioner.class);

        job2.waitForCompletion(true);




        //        Configuration conf = new Configuration();
//        conf.set("mapreduce.totalorderpartitioner.naturalorder", "false");
//        Job job = Job.getInstance(conf, "task6-1");
//        job.setJarByClass(TaskSix.class);
//
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        //设置比较器，用于比较数据的大小，然后按顺序排序，该例子主要用于比较两个key的大小
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//
//        //设置保存partitions文件的路径
//        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("partitions.txt"));
//    //    Path partitionFile = new Path(new Path(args[0]), "_partitions");
//    //    TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
//
//        //key值采样，0.01是采样率，
//        InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.5, 1000, 100);
//        InputSampler.writePartitionFile(job, sampler);
//
//
//
//        //采样器：三个参数
//        /* 第一个参数 freq: 表示来一个样本，将其作为采样点的概率。如果样本数目很大
//         *第二个参数 numSamples：表示采样点最大数目为，我这里设置10代表我的采样点最大为10，如果超过10，那么每次有新的采样点生成时
//         * ，会删除原有的一个采样点,此参数大数据的时候尽量设置多一些
//         * 第三个参数 maxSplitSampled：表示的是最大的分区数：我这里设置100不会起作用，因为我设置的分区只有4个而已
//*/
//
//        job.setMapperClass(StepMapper.class);
//        //      job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
//        job.setReducerClass(StepTwoReducer.class);
//        job.setPartitionerClass(TotalOrderPartitioner.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//
//
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
