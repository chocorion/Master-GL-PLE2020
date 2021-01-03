import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Pair<K, V> {
	public K key;
	public V value;

	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}
}

public class TP6 {
	public static void tryToInsert(ArrayList<Pair<String, Integer>> array, int maxElement, String city,
			int population) {
		int size = array.size();

		for (int i = 0; i < maxElement; i++) {
			if (i >= size) {
				array.add(i, new Pair<>(city, population));
				break;
			}

			Pair<String, Integer> currentCity = array.get(i);

			if (population >= currentCity.value) {
				array.add(i, new Pair<>(city, population));
				break;
			}
		}

		while (array.size() > maxElement)
			array.remove(maxElement);
	}

	public static class TP6Mapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
		private ArrayList<Pair<String, Integer>> array;

		@Override
		protected void setup(Context context) {
			this.array = new ArrayList<>();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			MapWritable map = new MapWritable();
			array.stream().forEach(p -> map.put(new Text(p.key), new IntWritable(p.value)));

			context.write(NullWritable.get(), map);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] cityValue = value.toString().split(",");

			if (cityValue.length != 7)
				return;

			String population = cityValue[4];
			String cityName = cityValue[1];

			if (population.length() == 0 || population.equalsIgnoreCase("population"))
				return;

			TP6.tryToInsert(this.array, context.getConfiguration().getInt("topK", 10), cityName, Integer.valueOf(population));
		}

		
	}

	public static class TP6Reducer extends Reducer<NullWritable, MapWritable, Text, IntWritable> {
		private ArrayList<Pair<String, Integer>> topK;
		
		@Override
		protected void setup(Context context) {
			this.topK = new ArrayList<>();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Pair<String, Integer> p : this.topK)
				context.write(new Text(p.key), new IntWritable(p.value));
		}

		//@Override
		public void reduce(NullWritable uselessKey, Iterable<MapWritable> citiesMap, Context context) {
			for (MapWritable cities : citiesMap){
				for (Writable key : cities.keySet()) {
					String city = ((Text) key).toString();
					int population = ((IntWritable) cities.get(key)).get();
	
					TP6.tryToInsert(this.topK, context.getConfiguration().getInt("topK", 10), city, population);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String inputFile = "/users/robin/worldcitiespop.txt";
		Configuration conf = new Configuration();

		int topK = Integer.valueOf(args[0]);
		conf.setInt("topK", topK);
	
		Job job = Job.getInstance(conf, "TP6");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP6.class);
		
		job.setMapperClass(TP6Mapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(MapWritable.class);

		job.setReducerClass(TP6Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(inputFile));

		job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
