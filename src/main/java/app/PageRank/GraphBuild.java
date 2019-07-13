package app.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 将Task3的输出person [name,weight|name,weight|...]的形式转换成person pageRank [name,weight|name,weight|...]，便于迭代
 */
public class GraphBuild {
    public static class GraphBuildMapper extends Mapper<Object, Text, Text, Text> {
        private final Text K = new Text();
        private final Text V = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 设置默认page rank为1.0
            String pageRank = "1.0\t";
            String[] line = value.toString().split("\t");
            K.set(line[0]);
            V.set(pageRank + line[1]);
            // 输出格式：person pageRank [name,weight|name,weight|...]
            context.write(K, V);
        }
    }

    /**
     * args[0]:input file path，即Task3的输出地址
     * args[1]:output file path
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task4.1:GraphBuild");
        job.setJarByClass(GraphBuild.class);
        job.setMapperClass(GraphBuildMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
