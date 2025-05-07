
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class IPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private Text mostFrequentIP = new Text();
    private int maxCount = 0;
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable val : values) {
            total += val.get();
        }
        if (total > maxCount) {
            maxCount = total;
            mostFrequentIP.set(key); 
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(mostFrequentIP, new IntWritable(maxCount)); 
    }
}

