import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Wmapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer token = new StringTokenizer(value.toString(), " ");
        while (token.hasMoreTokens()) {
            String word = token.nextToken();
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
