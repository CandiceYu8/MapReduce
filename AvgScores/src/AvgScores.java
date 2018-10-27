import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class AvgScores {
    public static class GroupMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
            String[] IDScores = value.toString().split("\\s+");
            if(IDScores.length != 2) {
                System.err.println("Error: each line should be a studentID and a score.");
                System.err.println(IDScores.length);
                System.exit(2);
            }
//            LongWritable groupNum = new LongWritable(Long.valueOf(String.valueOf(key)));
            IntWritable groupNUm = new IntWritable((Integer.parseInt(IDScores[0])-1)/5+1);
            IntWritable Score = new IntWritable(Integer.parseInt(IDScores[1]));
            context.write(groupNUm, Score);
        }
    }

    public static class AvgReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
        private FloatWritable result = new FloatWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>.Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            int num = 0;
            for (IntWritable val : values) {
                sum += val.get();
                num += 1;
            }
            this.result.set((float) sum/num);
            context.write(key, result);
        }
    }

    public static void main (String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: avg scores <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "avg score");
//        job.setInputFormatClass(NLineInputFormat.class);
//        NLineInputFormat.setNumLinesPerSplit(job, 5);

        job.setJarByClass(AvgScores.class);
        job.setMapperClass(AvgScores.GroupMapper.class);
        job.setReducerClass(AvgScores.AvgReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length-1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
