package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.DecDoubleWritable;

import java.io.IOException;
import java.net.URI;


public class TaskFour {

    private static final String PR_INIT = "1.0";

    private static final int LOOP_TIMES = 20;

    public static class StepOneMapper extends Mapper<Object, Text, Text, Text> {
        private final Text K = new Text();
        private final Text V = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] items = line.split("\t");

            if(!items[1].startsWith("[") || !items[1].endsWith("]" )){
                throw new RuntimeException();
            }
            String[] neighborsWithWeight = items[1].substring(1,items[1].length()-1).trim().split("|");
            String[] neighbors = new String[neighborsWithWeight.length];
            for(int i = 0;i<neighbors.length;i++){
                neighbors[i] = neighborsWithWeight[i].split(",")[0];
            }

            StringBuilder builder = new StringBuilder();
            builder.append(PR_INIT);
            builder.append('#');

            int builderBasicLength = builder.length();
            for(String neighbor :neighbors){
                builder.append(neighbor);
                builder.append('|');
            }
            if(builder.length()>builderBasicLength){
                builder.deleteCharAt(builder.length()-1);
            }

            context.write(new Text(items[0]),new Text(builder.toString()));
//
//            // 设置默认page rank为1.0
//            String pageRank = "1.0\t";
//            String[] line = value.toString().split("\t");
//            K.set(line[0]);
//            V.set(pageRank + line[1]);
//            // 输出格式：person pageRank [name,weight|name,weight|...]
//            context.write(K, V);
        }
    }

    public static class StepOneReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(key,value);
            }
        }
    }

    private static class StepTwoMapper extends Mapper<Object, Text, Text, Text> {
//        private final Text K = new Text();
//        private final Text V = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            // format: <role,currentPR#neighbor1|neighbor2|...>
            if (items.length != 2) {
                throw new RuntimeException();
            }
            String role = items[0];
            String[] prAndNeighbors = items[1].trim().split("#");
            if (prAndNeighbors.length != 2) {
                throw new RuntimeException();
            }

            // 迭代过程中保留链出信息
            context.write(new Text(role), new Text(prAndNeighbors[1]));

            String[] neighbors = prAndNeighbors[1].trim().split("|");
            double currentPr = Double.valueOf(prAndNeighbors[0]);
            double newPr = currentPr / neighbors.length;

            for (String neighbor : neighbors) {
                // '#' can be used to recognize type of this K-V pair
                context.write(new Text(neighbor), new Text(String.format("#%f", newPr)));
            }

//            String[] name_weights = items[2].replace("[", "")
//                    .replace("]", "")
//                    .split("\\|");
//            for (String name_weight : name_weights) {
//                String name = name_weight.split(",")[0];
//                double weight = Double.valueOf(name_weight.split(",")[1]);
//                K.set(name);
//                // "&"作为标记区分
//                V.set("&" + weight * currentPR);
//                context.write(K, V);
//            }
        }
    }

    private static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {
//        private final Text K = new Text();
//        private final Text V = new Text();

        private final double damping = 0.85;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            String neighbors = null;
            for (Text value : values) {
                if (value.toString().startsWith("#")) {
                    sum += Double.valueOf(value.toString().substring(1));
                } else {
                    if (neighbors != null) {
                        throw new RuntimeException();
                    }
                    neighbors = value.toString();
                }
            }
            if (neighbors == null) {
                throw new RuntimeException();
            }

            double newPr = 1 - damping + damping * sum;

            context.write(key,new Text(String.format("%f#%s",newPr,neighbors)));


            //            double pageRank = 0;
//            String name_weights = "";
//            for (Text v : values) {
//                String line = v.toString();
//                if (line.contains("&")) {
//                    pageRank += Double.valueOf(line.split("&")[1]);
//                } else {
//                    name_weights = line;
//                }
//            }
//            K.set(key);
//            V.set(String.valueOf(0.15 + 0.85 * pageRank) + "\t" + name_weights);
//            // 输出格式：person pageRank [name,weight|name,weight|...]
//            context.write(K, V);
        }

    }

    private static class StepThreeMapper extends Mapper<Object,Text, DecDoubleWritable,Text>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            if (items.length != 2) {
                throw new RuntimeException();
            }
            String role = items[0];
            String[] prAndNeighbors = items[1].trim().split("#");
            if (prAndNeighbors.length != 2) {
                throw new RuntimeException();
            }

            context.write(new DecDoubleWritable(Double.valueOf(prAndNeighbors[0])),new Text(role));
        }
    }

    private static class StepThreeReducer extends Reducer<DecDoubleWritable, Text, DecDoubleWritable, Text>{
        @Override
        public void reduce(DecDoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Task Four Step One");
        job1.setJarByClass(TaskFour.class);
        job1.setMapperClass(TaskFour.StepOneMapper.class);
        job1.setReducerClass(TaskFour.StepOneReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));

        String stepTwoInput = String.format("%s/step1_out",args[1]);
        FileOutputFormat.setOutputPath(job1, new Path(stepTwoInput));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);


        for(int i = 0;i<LOOP_TIMES;i++){
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Task Four Step Two");
            job2.setJarByClass(TaskFour.class);
            job2.setMapperClass(TaskFour.StepTwoMapper.class);
            job2.setReducerClass(TaskFour.StepTwoReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(stepTwoInput));

            String path = String.format("%s/loop_%d",args[1],i);
            FileOutputFormat.setOutputPath(job2, new Path(path));
            stepTwoInput = path;
        }

        String stepThreeInput = stepTwoInput;
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Task Four Step Three");
        job3.setJarByClass(TaskFour.class);
        job3.setMapperClass(TaskFour.StepThreeMapper.class);
        job3.setReducerClass(TaskFour.StepThreeReducer.class);
        job3.setMapOutputKeyClass(DecDoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(DecDoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(stepThreeInput));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

    }
}
