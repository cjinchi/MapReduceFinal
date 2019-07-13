package app.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Task4: 数据分析：基于人物关系图的PageRank计算
 */

public class PageRank {

    private static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        private final Text K = new Text();
        private final Text V = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // format: person pageRank [name,weight|name,weight|...]
            String[] line = value.toString().split("\t");
            String person = line[0];
            double pageRank = Double.valueOf(line[1]);
            K.set(person);
            V.set(line[2]);
            // 迭代过程中保留链出信息
            // format: person [name,weight|name,weight|...]
            context.write(K, V);

            String[] name_weights = line[2].replace("[", "")
                    .replace("]", "")
                    .split("\\|");
            for (String name_weight : name_weights) {
                String name = name_weight.split(",")[0];
                double weight = Double.valueOf(name_weight.split(",")[1]);
                K.set(name);
                // "&"作为标记区分
                V.set("&" + weight * pageRank);
                context.write(K, V);
            }
        }
    }

    private static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private final Text K = new Text();
        private final Text V = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pageRank = 0;
            String name_weights = "";
            for (Text v : values) {
                String line = v.toString();
                if (line.contains("&")) {
                    pageRank += Double.valueOf(line.split("&")[1]);
                } else {
                    name_weights = line;
                }
            }
            K.set(key);
            V.set(String.valueOf(0.15 + 0.85 * pageRank) + "\t" + name_weights);
            // 输出格式：person pageRank [name,weight|name,weight|...]
            context.write(K, V);
        }

    }


    /**
     * args[0]:input file path，即GraphBuild处理后的文件路径
     * args[1]:output file path
     * args[2]:迭代次数
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String IN = args[0], OUT;
        int LOOP_TIMES = Integer.valueOf(args[2]);

        for (int i = 0; i < LOOP_TIMES; i++) {
            //以下配置均参考自官方文档
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Task4:PageRank_Loop" + i);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRank.PageRankMapper.class);
            job.setReducerClass(PageRank.PageRankReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(IN));
            OUT = args[1] + "_Loop" + i;
            FileOutputFormat.setOutputPath(job, new Path(OUT));
            IN = OUT;
            job.waitForCompletion(false);
        }
    }
}
