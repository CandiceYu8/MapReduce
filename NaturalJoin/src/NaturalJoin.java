import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NaturalJoin {
    public static class PersonMapper extends Mapper<Object, Text, Text, Text>{
        private static final String PERSON_FLAG = "person";
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException{
            String[] info = value.toString().split("\\s+");
            if(info.length == 3){
                context.write(new Text(info[2]), new Text(PERSON_FLAG + " " + info[0] + " " + info[1]));
            }
        }
    }

    public static class AddressMapper extends Mapper<Object, Text, Text, Text>{
        private static final String ADDRESS_FLAG = "address";
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException{
            String[] addr = value.toString().split("\\s+");
            if(addr.length == 2){
                context.write(new Text(addr[0]), new Text(ADDRESS_FLAG + " " + addr[1]));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        private static final String PERSON_FLAG = "person";
        private static final String ADDRESS_FLAG = "address";
        private String fileFlag = null;
        private String city = null;
        private List<String> summary;
        public void reduce(Text key, Iterable<Text>value, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException{
            summary = new ArrayList<>();
            for (Text val: value) {
                String[] fields = val.toString().split("\\s+");
                fileFlag = fields[0];
                if(fileFlag.equals(PERSON_FLAG)){
                    summary.add(fields[1] + " " + key.toString() + " " + fields[2]);
                }
                else if(fileFlag.equals(ADDRESS_FLAG)){
                    city = fields[1];
                }
            }
            for (String personInfo: summary) {
                context.write(new Text(personInfo), new Text(city));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: natural join <person.txt> <address.txt> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "natural join");
        job.setJarByClass(NaturalJoin.class);
        job.setReducerClass(NaturalJoin.JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, NaturalJoin.PersonMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, NaturalJoin.AddressMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
