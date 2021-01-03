import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {

	public static class TP5Mapper extends Mapper<LongWritable, Point2DWritable, DoubleWritable, DoubleWritable>{
		public void map(LongWritable arg0, Point2DWritable point, Context context) throws IOException, InterruptedException {
			context.write(new DoubleWritable(point.x), new DoubleWritable(point.y));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		
		Job job = Job.getInstance(conf, "TP5");
		job.setNumReduceTasks(0);
		job.setJarByClass(TP5.class);
		
		RandomPointInputFormat.setNumberOfSplit(4);
		RandomPointInputFormat.setNumberOfPointPerSplit(100);
		job.setInputFormatClass(RandomPointInputFormat.class);

		job.setMapperClass(TP5Mapper.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		job.waitForCompletion(true);
	}
}
