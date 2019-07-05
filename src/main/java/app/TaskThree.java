package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;


public class TaskThree {

    public static class TaskThreeMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            Text name = new Text();
            IntWritable number = new IntWritable();
            if (itr.hasMoreTokens())
                name.set(itr.nextToken());
            if (itr.hasMoreTokens())
                number.set(Integer.parseInt(itr.nextToken()));
            context.write(name, number);
        }

    }

    public static class TaskThreePartitioner extends HashPartitioner<Text, IntWritable> {
        private Text term = new Text();

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            term.set(key.toString().split(",")[0]);
            return super.getPartition(term, value, numReduceTasks);
        }
    }

    public static class TaskThreeReducer extends Reducer<Text, IntWritable, Text, Text> {
        //类似于temp变量，用于把String转为Text
        private Text str1 = new Text();
        private Text str2 = new Text();

        /**
         * 由于一个key [即term+filename]调用一次reduce()函数，因此同一个term实际上需要调用多次reduce函数才能处理完毕。
         * currentTerm表示当前正在处理的term，当reduce函数中判断到term和currentTerm不相同时，说明已经开始处理下一个term了。
         * currentPostings表示currentTerm的索引，当currentTerm变化时需要清空。
         */
        private String previousName = null;
        private int currentCount = 0;


        private Map<String, Integer> postings = new LinkedHashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String twoNames = key.toString();
            String[] names = twoNames.substring(1, twoNames.length() - 1).split(",");
            String firstName = names[0];
            String secondName = names[1];
            if (previousName == null || !previousName.equals(firstName)) {
                writeCurrentName(context);
                previousName = firstName;
                postings.clear();
                currentCount = 0;
            }
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            currentCount = currentCount + sum;
            postings.put(secondName, sum);

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
                writeCurrentName(context);
        }

        private void writeCurrentName(Context context) throws IOException, InterruptedException {
            if(previousName == null){
                return;
            }
            double percent = 1.0 / currentCount;
            StringBuilder postingsBuilder = new StringBuilder();
            postingsBuilder.append('[');
            int index = 0;
            for (Map.Entry<String, Integer> entry : postings.entrySet()) {
                //     System.out.println("key="+entry.getKey()+",value = "+entry.getValue());
                if (index == 0)
                    index = 1;
                else
                    postingsBuilder.append('|');
                postingsBuilder.append(entry.getKey());
                postingsBuilder.append(',');
                postingsBuilder.append(entry.getValue() * percent);
            }
            postingsBuilder.append(']');
            str1.set(previousName);
            str2.set(postingsBuilder.toString());
            System.out.println(previousName+postingsBuilder.toString());
            context.write(str1, str2);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //以下配置均参考自官方文档
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(TaskThree.class);
        job.setMapperClass(TaskThree.TaskThreeMapper.class);
        //      job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
        job.setReducerClass(TaskThree.TaskThreeReducer.class);
        job.setPartitionerClass(TaskThree.TaskThreePartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
