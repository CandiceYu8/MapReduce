import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
    private List<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();
    protected void setup(Context context) throws IOException {
        String centerPath = context.getLocalCacheFiles()[0].getName();
//        String centerPath = "input/centers.txt";
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream fsIn = hdfs.open(new Path(centerPath));
        LineReader lineIn = new LineReader(fsIn, conf);
        Text line = new Text();
        while (lineIn.readLine(line) > 0){
            ArrayList<Double> tmp = new ArrayList<>();
            String[] dots = line.toString().split(",");
            for (int i = 0; i < dots.length; i++) {
                tmp.add(Double.parseDouble(dots[i]));
            }
            centers.add(tmp);
        }
    }

    public void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
            throws IOException, InterruptedException{
        if(key.toString().equals("0"))
            return;
        String[] dot = value.toString().split(",");
        Double dotX = Double.parseDouble(dot[0]);
        Double dotY = Double.parseDouble(dot[1]);
        /* judge cluster */
        double minDis = 999999;
        double dis = 0;
        int cluster = 0;
        for (int i = 0; i < 4; i++) {
            double tmp1 = Math.pow(dotX - centers.get(i).get(0), 2);
            double tmp2 = Math.pow(dotY - centers.get(i).get(1), 2);
            dis = Math.sqrt(tmp1+tmp2);
            if(dis < minDis){
                minDis = dis;
                cluster = i;
            }
        }
        context.write(new IntWritable(cluster), value);
    }
}