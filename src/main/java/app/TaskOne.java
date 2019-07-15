package app;

import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class TaskOne {

    private final static String TAG_NAME = "jyxsrw";
    private final static String DIC_FILE_LABEL = "NAME_FILE";

    public static class TaskOneMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles.length < 1) {
                throw new RuntimeException("Error before load dictionary from cache files.");
            }
            BufferedReader reader = new BufferedReader(new FileReader(DIC_FILE_LABEL));
            String temp;
            while ((temp = reader.readLine()) != null && temp.trim().length() > 0) {
                DicLibrary.insert(DicLibrary.DEFAULT, temp.trim(), TAG_NAME, Integer.MAX_VALUE);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<Term> terms = DicAnalysis.parse(value.toString()).getTerms();
            StringBuilder builder = new StringBuilder();
            for (Term term : terms) {
                if (term.getNatureStr().equals(TAG_NAME)) {
                    builder.append(term.getName());
                    builder.append(" ");
                }
            }
            if (builder.length() > 0) {
                builder.deleteCharAt(builder.length() - 1);
                context.write(new Text(builder.toString()), new Text());
            }

        }
    }

    public static class TaskOneReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {

        if (args.length < 3) {
            throw new RuntimeException("Please specify [dictionary] [input] [output]");
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task One");
        job.setJarByClass(TaskOne.class);
        job.setMapperClass(TaskOne.TaskOneMapper.class);
        job.setReducerClass(TaskOne.TaskOneReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI(args[0] + "#" + DIC_FILE_LABEL));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
