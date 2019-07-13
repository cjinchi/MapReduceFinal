package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

public class TaskFive{
    private final static String DIC_FILE_LABEL = "NAME_FILE";

    public static class TaskFiveStepOneMapper extends Mapper<Object, Text, Text, Text> {
        private Map<String, String> roleNameToId = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
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
            String sharpRoleId = String.format("#%s", roleNameToId.get(roleName));
            // Type 1: <roleName,#roleId>
            context.write(new Text(roleName), new Text(sharpRoleId));

            String[] neighbors = items[1].substring(1, items[1].length() - 1).split("|");
            for (int i = 0; i < neighbors.length; i++) {
                neighbors[i] = neighbors[i].trim().split(",")[0];
                // format as neighborName#neighborId
                String neighborNameSharpId = String.format("%s#%s", neighbors[i], roleNameToId.get(neighbors[i]));
                // Type 2: <roleName, neighborName#neighborId>
                context.write(new Text(roleName), new Text(neighborNameSharpId));
            }

            //Note:In next step, Type one will be written by "role", Type two will be written by "neighbor"
        }
    }

    public static class TaskFiveStepOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(key,value);
            }
        }
    }

    public static class TaskFiveStepTwoMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class TaskFiveStepTwoReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String roleName = key.toString();
            String roleId = null;
            Map<String, Integer> idCounter = new HashMap<>();
            List<String> neighborsOfCurrentRole = new ArrayList<>();

            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("#")) {
                    roleId = valueStr.substring(1, valueStr.length());
                } else {
                    String[] items = valueStr.split("#");
                    neighborsOfCurrentRole.add(items[0]);
//                    idCounter.put(items[1],idCounter.getOrDefault(items[1],0)+1);
                    if (idCounter.containsKey(items[1])) {
                        idCounter.put(items[1], idCounter.get(items[1]));
                    } else {
                        idCounter.put(items[1], 1);
                    }
                }
            }

            //Find id that most neighbor have
            List<Map.Entry<String, Integer>> maxIdsWithCount = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : idCounter.entrySet()) {
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
            if (roleId == null || maxIdsWithCount.isEmpty()) {
                throw new RuntimeException();
            }

            //Check if current role should change its id. If so, no need to continue iteration.
            boolean roleIdShouldChange = true;
            for(Map.Entry<String,Integer> entry:maxIdsWithCount){
                if(roleId.equals(entry.getKey())){
                    roleIdShouldChange = false;
                    break;
                }
            }

            if(roleIdShouldChange){
                //TODO:report change here

                //"ties are broken uniformly randomly"
                Collections.shuffle(maxIdsWithCount);
                roleId = maxIdsWithCount.get(0).getKey();
            }

            context.write(new Text(roleName),new Text(String.format("#%s",roleId)));
            Text roleSharpId = new Text(String.format("%s#%s",roleName,roleId));
            for(String neighbor:neighborsOfCurrentRole){
                context.write(new Text(neighbor),roleSharpId);
            }

        }
    }

    public static class TaskFiveStepThreeMapper extends Mapper<Text, Text, Text, Text>{
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if(value.toString().startsWith("#")){
                context.write(key,new Text(value.toString().substring(1)));
            }
        }
    }

    public static class TaskFiveStepThreeReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            throw new RuntimeException("Please specify [dictionary] [input] [output]");
        }
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Task Five");
//        job.setJarByClass(TaskFiveStepOne.class);
//        job.setMapperClass(TaskFiveStepOne.TaskFiveStepOneMapper.class);
//        job.setReducerClass(TaskFiveStepOne.TaskFiveStepOneReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.addCacheFile(new URI(args[0] + "#" + DIC_FILE_LABEL));
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
