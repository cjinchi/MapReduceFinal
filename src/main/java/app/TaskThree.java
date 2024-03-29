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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public class TaskThree {

    public static class TaskThreeMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            if (items.length != 2) {
                throw new RuntimeException();
            }
            context.write(new Text(items[0]), new IntWritable(Integer.valueOf(items[1])));
        }

    }

    public static class TaskThreePartitioner extends HashPartitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String[] items = key.toString().trim().split(",");
            if (items.length != 2 || !items[0].startsWith("<")) {
                throw new RuntimeException();
            }
            return super.getPartition(new Text(items[0].substring(1)), value, numReduceTasks);
        }
    }

    public static class TaskThreeReducer extends Reducer<Text, IntWritable, Text, Text> {
        private String currentName = null;
        private int currentCount = 0;
        private Map<String, Integer> postings = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString();
            if(!keyStr.startsWith("<")||!keyStr.endsWith(">") ){
                throw new RuntimeException();
            }
            String[] items = keyStr.substring(1, keyStr.length()-1).split(",");
            if(items.length!=2){
                throw new RuntimeException();
            }

            if(currentName == null){
                currentName = items[0];
                currentCount = 0;
                postings.clear();
            }else if(!currentName.equals(items[0])){
                writeCurrentName(context);

                currentName = items[0];
                currentCount = 0;
                postings.clear();
            }

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            currentCount+=sum;
            postings.put(items[1], sum);

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            writeCurrentName(context);
        }

        private void writeCurrentName(Context context) throws IOException, InterruptedException {
            if (currentName == null) {
                return;
            }
            double total = currentCount;
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            for (Map.Entry<String, Integer> entry : postings.entrySet()) {
                builder.append(entry.getKey());
                builder.append(',');
                builder.append(entry.getValue() / total);
                builder.append('|');
            }
            if(builder.length()>1){
                builder.deleteCharAt(builder.length()-1);
            }
            builder.append(']');
            context.write(new Text(currentName),new Text(builder.toString()));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task Three");
        job.setJarByClass(TaskThree.class);
        job.setMapperClass(TaskThreeMapper.class);
        job.setReducerClass(TaskThreeReducer.class);
        job.setPartitionerClass(TaskThreePartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
