package bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
	public static class TP3Mapper extends Mapper<Object, Text, Text, IntWritable> {

		static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

			// Country,City,AccentCity,Region,Population,Latitude,Longitude
			String line = value.toString();
			String[] splitted = line.split(",");
			context.getCounter("tp3", "nbCities").increment(1);

			if (splitted[4].length() == 0 || splitted[4].equals("Population"))
				return;

			context.getCounter("tp3", "nbValidCities").increment(1);
			context.getCounter("tp3", "totalPop").increment(Long.valueOf(splitted[4]));
			
			context.write(value, one);
		}
	}


	public static class TP3Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
				
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TP3");

		job.setNumReduceTasks(0);
		job.setJarByClass(TP3.class);
		
		job.setMapperClass(TP3Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(TP3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/users/robin/worldcitiespop.txt"));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
