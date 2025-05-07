
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IPDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job  = Job.getInstance(conf, "IPSHOW");
		job.setJarByClass(IPDriver.class);
		job.setMapperClass(IPMapper.class);
		job.setReducerClass(IPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/dir3101/access_log_short.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/dir3101/DBoutput"));
		job.waitForCompletion(true);
		
	}
}
