//package app.PageRank;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.IOException;
//
///**
// * 将Task3的输出person [name,weight|name,weight|...]的形式转换成person pageRank [name,weight|name,weight|...]，便于迭代
// */
//public class GraphBuild {
//    private static final String PR_INIT = "1.0";
//
//    public static class GraphBuildMapper extends Mapper<Object, Text, Text, Text> {
//        private final Text K = new Text();
//        private final Text V = new Text();
//
//        @Override
//        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString().trim();
//            String[] items = line.split("\t");
//
//            if(!items[1].startsWith("[") || !items[1].endsWith("]" )){
//                throw new RuntimeException();
//            }
//            String[] neighborsWithWeight = items[1].substring(1,items[1].length()-1).trim().split("|");
//            String[] neighbors = new String[neighborsWithWeight.length];
//            for(int i = 0;i<neighbors.length;i++){
//                neighbors[i] = neighborsWithWeight[i].split(",")[0];
//            }
//
//            StringBuilder builder = new StringBuilder();
//            builder.append(PR_INIT);
//            builder.append('#');
//
//            int builderBasicLength = builder.length();
//            for(String neighbor :neighbors){
//                builder.append(neighbor);
//                builder.append('|');
//            }
//            if(builder.length()>builderBasicLength){
//                builder.deleteCharAt(builder.length()-1);
//            }
//
//            context.write(new Text(items[0]),new Text(builder.toString()));
////
////            // 设置默认page rank为1.0
////            String pageRank = "1.0\t";
////            String[] line = value.toString().split("\t");
////            K.set(line[0]);
////            V.set(pageRank + line[1]);
////            // 输出格式：person pageRank [name,weight|name,weight|...]
////            context.write(K, V);
//        }
//    }
//
//    public static class GraphBuildReducer extends Reducer<Text,Text,Text,Text>{
//        @Override
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            for(Text value:values){
//                context.write(key,value);
//            }
//        }
//    }
//
//
//    /**
//     * args[0]:input file path，即Task3的输出地址
//     * args[1]:output file path
//     */
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Task4.1:GraphBuild");
//        job.setJarByClass(GraphBuild.class);
//        job.setMapperClass(GraphBuildMapper.class);
//        job.setReducerClass(GraphBuildReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.waitForCompletion(true);
//    }
//}
