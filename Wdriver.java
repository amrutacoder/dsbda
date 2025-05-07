import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Wdriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		Job job  = Job.getInstance(conf, "word_count");
		job.setJarByClass(Wdriver.class);
		job.setMapperClass(Wmapper.class);
		job.setReducerClass(Wreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/dir3101/first.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/dir3101/op.txt"));
		job.waitForCompletion(true);
		
	}
}
