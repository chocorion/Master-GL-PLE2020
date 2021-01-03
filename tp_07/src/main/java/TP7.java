import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TP7 {
	public static class MapperCities extends Mapper<LongWritable, Text, Text, TaggedValue> {
		@Override
		protected void map(LongWritable key, Text value, Context context) {
			TaggedValue.fromCityData(value.toString()).ifPresent(taggedValue -> {
				taggedValue.getCountryCode().ifPresent((ThrowingConsumer<String>) code -> {
						context.write(new Text(code), taggedValue);
				});
			});
		}
	}

	public static class MapperCodes extends Mapper<LongWritable, Text, Text, TaggedValue> {
		@Override
		protected void map(LongWritable key, Text value, Context context) {
			TaggedValue taggedValue = TaggedValue.fromCodeData(value.toString());

			taggedValue.getCountryCode().ifPresent((ThrowingConsumer<String>) code -> {
					context.write(new Text(code), taggedValue);
			});
		}
	}


	public static class CityCodeReducer extends Reducer<Text, TaggedValue, Text, Text> {

		public void reduce(Text countryCode, Iterable<TaggedValue> taggedValues, Context context) throws IOException, InterruptedException {
			List<TaggedValue> cities = new ArrayList<>();
			Text regionName = null;

			Iterator<TaggedValue> iterator = taggedValues.iterator();

			while (iterator.hasNext()) {
				TaggedValue tmp = iterator.next();

				if (tmp.isCity)
					cities.add(tmp);
				else {
					if (tmp.getRegionName().isPresent())
						regionName = new Text(tmp.getRegionName().get());
					else
						return;
					break;
				}
			}

			for (TaggedValue city : cities) {
				if (city.getCityName().isPresent()) {
					context.write(new Text(city.getCityName().get()), regionName);
				}
			}

			while (iterator.hasNext()) {
				TaggedValue tmp = iterator.next();

				if (tmp.getCityName().isPresent()) {
					context.write(new Text(tmp.getCityName().get()), regionName);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String inputFileCities = "/raw_data/worldcitiespop.txt";
		String intputFileCodes = "/raw_data/region_codes.csv";

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "TP7");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP7.class);

		MultipleInputs.addInputPath(job, new Path(inputFileCities), TextInputFormat.class, MapperCities.class);
		MultipleInputs.addInputPath(job, new Path(intputFileCodes), TextInputFormat.class, MapperCodes.class);

		job.setReducerClass(CityCodeReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TaggedValue.class);

		job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(args[0]));

		job.waitForCompletion(true);
	}
}
