package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class TaskFive {
    private final static String DIC_FILE_LABEL = "NAME_FILE";

    private final static int LOOP_TIMES = 40;

    public static class StepOneMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> roleNameToId = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles.length < 1) {
                throw new RuntimeException("Error before load dictionary from cache files.");
            }
            BufferedReader reader = new BufferedReader(new FileReader(DIC_FILE_LABEL));
            int i = 0;
            String temp;
            while ((temp = reader.readLine()) != null) {
                roleNameToId.put(temp.trim(), String.valueOf(i++));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            if (items.length != 2) {
                throw new RuntimeException("Incorrect format: input of TaskFive.");
            }
            String roleName = items[0];
            String sharpRoleIdSharpNeighbors = String.format("#%s#%s", roleNameToId.get(roleName), items[1].substring(1, items[1].length() - 1));
            // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
            context.write(new Text(roleName), new Text(sharpRoleIdSharpNeighbors));

            String[] neighbors = items[1].substring(1, items[1].length() - 1).split("\\|");
            for (int i = 0; i < neighbors.length; i++) {
                String neighborName = neighbors[i].trim().split(",")[0];
                // format as neighborName#neighborId
                String neighborNameSharpId = String.format("%s#%s", neighborName, roleNameToId.get(neighborName));
                // Type 2: <roleName, neighborName#neighborId>
                context.write(new Text(roleName), new Text(neighborNameSharpId));
            }

            //Note:In next step, Type one will be written by "role", Type two will be written by "neighbor"
        }
    }

    public static class StepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static class StepTwoMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            if (items.length != 2) {
                throw new RuntimeException();
            }
            context.write(new Text(items[0]), new Text(items[1]));
        }
    }

    public static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String roleName = key.toString();
            String roleId = null;

            Map<String, String> neighborNameToId = new HashMap<>();
            Map<String, Double> neighborNameToWeight = new HashMap<>();
            Map<String,Double> idCounter = new HashMap<>();
            String neighborInformation = null;

            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("#")) {
                    // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
                    String[] items = valueStr.substring(1).trim().split("#");
                    if (items.length != 2) {
                        throw new RuntimeException();
                    }
                    roleId = items[0];
                    neighborInformation = items[1];
                    for (String neighborCommaWeight : items[1].split("\\|")) {
                        String[] neighborNameAndWeight = neighborCommaWeight.split(",");
                        if (neighborNameAndWeight.length != 2) {
                            throw new RuntimeException();
                        }
                        neighborNameToWeight.put(neighborNameAndWeight[0], Double.valueOf(neighborNameAndWeight[1]));
                    }
                } else {
                    // Type 2: <roleName, neighborName#neighborId>
                    String[] neighborNameAndId = valueStr.split("#");
                    if (neighborNameAndId.length != 2) {
                        throw new RuntimeException();
                    }
                    neighborNameToId.put(neighborNameAndId[0], neighborNameAndId[1]);
                }
            }

            for(Map.Entry<String,String> entry:neighborNameToId.entrySet()) {
                double weight = neighborNameToWeight.get(entry.getKey());
                if (idCounter.containsKey(entry.getValue())) {
                        idCounter.put(entry.getValue(), idCounter.get(entry.getValue()) + weight);
                } else {
                    idCounter.put(entry.getValue(), weight);
                }
            }

            //Find id with largest weight
            List<Map.Entry<String, Double>> maxIdsWithCount = new ArrayList<>();
            for (Map.Entry<String, Double> entry : idCounter.entrySet()) {
                if (maxIdsWithCount.isEmpty()) {
                    maxIdsWithCount.add(entry);
                } else if (entry.getValue().equals(maxIdsWithCount.get(0).getValue())) {
                    maxIdsWithCount.add(entry);
                } else if (entry.getValue() > maxIdsWithCount.get(0).getValue()) {
                    maxIdsWithCount.clear();
                    maxIdsWithCount.add(entry);
                } else {
                    //do nothing
                }
            }

            //Error check
            if (roleId == null || maxIdsWithCount.isEmpty() || neighborInformation == null) {
                throw new RuntimeException();
            }

//            //Check if current role should change its id. If so, no need to continue iteration.
            boolean roleIdShouldChange = true;
            for (Map.Entry<String, Double> entry : maxIdsWithCount) {
                if (roleId.equals(entry.getKey())) {
                    roleIdShouldChange = false;
                    break;
                }
            }

            if (roleIdShouldChange) {
                //"ties are broken uniformly randomly"
                Collections.shuffle(maxIdsWithCount);
                roleId = maxIdsWithCount.get(0).getKey();
            }

            // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
            context.write(new Text(roleName), new Text(String.format("#%s#%s", roleId,neighborInformation)));

            // Type 2: <roleName, neighborName#neighborId>
            Text roleSharpId = new Text(String.format("%s#%s", roleName, roleId));
            for (String neighbor : neighborNameToId.keySet()) {
                context.write(new Text(neighbor), roleSharpId);
            }

        }
    }

    public static class StepThreeMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().trim().split("\t");
            if (items.length != 2) {
                throw new RuntimeException();
            }
            context.write(new Text(items[0]), new Text(items[1]));
        }
    }

    public static class StepThreeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if(value.toString().startsWith("#")){
                    context.write(key,new Text(value.toString().trim().substring(1).split("#")[0]));
                }
                // Type 1: <roleName,#roleId#neighbor1,weight|neighbor2,weight|...>
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        if (args.length < 4) {
            throw new RuntimeException("Please specify [dictionary] [input] [inter] [output]");
        }
        String dic = args[0];
        String input = args[1];
        String inter = args[2];
        String output = args[3];

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Task Five Step One");
        job1.setJarByClass(TaskFive.class);
        job1.setMapperClass(StepOneMapper.class);
        job1.setReducerClass(StepOneReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.addCacheFile(new URI(dic + "#" + DIC_FILE_LABEL));
        FileInputFormat.addInputPath(job1, new Path(input));
        String stepTwoInput = String.format("%s/step1_out", inter);
        FileOutputFormat.setOutputPath(job1, new Path(stepTwoInput));
        job1.waitForCompletion(true);

        for (int i = 0; i < LOOP_TIMES; i++) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, String.format("Task Five Step Two Round %d", i));
            job2.setJarByClass(TaskFive.class);
            job2.setMapperClass(StepTwoMapper.class);
            job2.setReducerClass(StepTwoReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(stepTwoInput));

            String path = String.format("%s/loop_%d", inter, i);
            FileOutputFormat.setOutputPath(job2, new Path(path));
            stepTwoInput = path;

            job2.waitForCompletion(true);
        }

        String stepThreeInput = stepTwoInput;
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Task Five Step Three");
        job3.setJarByClass(TaskFive.class);
        job3.setMapperClass(StepThreeMapper.class);
        job3.setReducerClass(StepThreeReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(stepThreeInput));
        FileOutputFormat.setOutputPath(job3, new Path(output));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }


}
