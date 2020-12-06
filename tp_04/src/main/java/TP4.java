package bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP4 {
	public static class TP4Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		int step;

		public void setup(Context context) {
			step = context.getConfiguration().getInt("step", 5);
		}

		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

			// Country,City,AccentCity,Region,Population,Latitude,Longitude
			String line = value.toString();
			String[] splitted = line.split(",");

			if (splitted[4].length() == 0 || splitted[4].equals("Population"))
				return;
			
			
			int cityClass = (int) Math.pow(
				step,
				Math.floor(  Math.log(Integer.valueOf(splitted[4])) / Math.log(step)  )
			);
			context.write(new IntWritable(cityClass), new IntWritable(Integer.valueOf(splitted[4])));
		}
	}


	public static class TP4Reducer extends Reducer<IntWritable,IntWritable,Text,NullWritable> {

		public void setup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Class       Count      Avg       Max      Min"), NullWritable.get());

		}

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
				
			int count = 0;
			int max = Integer.MIN_VALUE;
			int min = Integer.MAX_VALUE;
			int sum = 0;
				
			for (IntWritable i : values) {
				int value = i.get();
				count++;
				sum += value;

				max = (value > max)? value : max;
				min = (value < min)? value : min;
			}

			String result = String.format("%9d %5d %9d %9d %9d", key.get(), count, sum/count, max, min);

			context.write(new Text(result), NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TP4");

		conf.setInt("step", Integer.parseInt(args[0]));

		job.setNumReduceTasks(1);
		job.setJarByClass(TP4.class);
		
		job.setMapperClass(TP4Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(TP4Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/users/robin/worldcitiespop.txt"));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
