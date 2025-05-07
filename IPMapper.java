import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class IPMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer token = new StringTokenizer(value.toString(), " ");
        
            String word = token.nextToken();
            context.write(new Text(word), new IntWritable(1));
        
    }
}
