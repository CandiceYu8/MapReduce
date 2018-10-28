import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeans {
    static void runKMeans(String[] otherArgs)
        throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kmeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(otherArgs[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }

    static boolean convergence(String oldCenters, String newCenters, double threshold) throws IOException{
//        String oldCenters = "input/centers.txt";
        String newCentersFile = newCenters + "/part-r-00000";

        List<Double> oldC = new ArrayList<>();
        List<Double> newC = new ArrayList<>();
        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        FSDataInputStream fsIn = hdfs.open(new Path(oldCenters));
        LineReader lineIn = new LineReader(fsIn, conf);
        Text line1 = new Text();
        while (lineIn.readLine(line1) > 0){
            String[] dots1 = line1.toString().split(",");
            oldC.add(Double.parseDouble(dots1[0]));
            oldC.add(Double.parseDouble(dots1[1]));
        }

        FSDataInputStream fsIn2 = hdfs.open(new Path(newCentersFile));
        LineReader lineIn2 = new LineReader(fsIn2, conf);
        Text line2 = new Text();
        while (lineIn2.readLine(line2) > 0){
            String[] dots2 = line2.toString().split(",");
            newC.add(Double.parseDouble(dots2[0]));
            newC.add(Double.parseDouble(dots2[1]));
        }

        double error = 0;
        for (int i = 0; i < 8; i+=2) {
            double tmp1 = Math.pow(oldC.get(i)-newC.get(i), 2);
            double tmp2 = Math.pow(oldC.get(i+1)-newC.get(i+1), 2);
            error += Math.sqrt(tmp1+tmp2);
        }

        if(error <= threshold){
            return true;
        }
        else{
            boolean tmp = hdfs.delete(new Path(oldCenters));
            System.out.println("delete centers: " + tmp);
            tmp = hdfs.rename(new Path(newCentersFile), new Path(oldCenters));
            System.out.println("rename: " + tmp);
            tmp = hdfs.delete(new Path(newCenters));
            System.out.println("delete output: " + tmp);
            return false;
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("Usage: KMeans <in> <out> <cache>");
            System.exit(2);
        }

        int numIter = 0;
        final int maxIter = 100;
        runKMeans(otherArgs);
        while (true){
            System.out.println("Iteration number: " + numIter);
            if(!convergence(otherArgs[2], otherArgs[1], 0.001) && numIter < maxIter){
                runKMeans(otherArgs);
                numIter++;
            }
            else{
                break;
            }
        }
    }
}
