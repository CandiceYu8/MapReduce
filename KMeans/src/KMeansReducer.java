import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Text, Text, Text> {
    public void reduce(IntWritable key, Iterable<Text> value, Reducer<IntWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException{
        int num = 0;
        double x = 0;
        double y = 0;
        for (Text val: value) {
            String[] dotTmp = val.toString().split(",");
            num++;
            x += Double.parseDouble(dotTmp[0]);
            y += Double.parseDouble(dotTmp[1]);
        }
        String tmp = x/num + "," + y/num;
        Text outkey = new Text(tmp);
        context.write(outkey, null);
    }
}