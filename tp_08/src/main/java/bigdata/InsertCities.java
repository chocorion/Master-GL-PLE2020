package bigdata;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class InsertCities extends Configured implements Tool {
    private static final byte[] TABLE_NAME = Bytes.toBytes("rnavarro-td8");
    private static final byte[][] FAMILIES = {
        Bytes.toBytes("loc"),
        Bytes.toBytes("measure"),
        Bytes.toBytes("name"),
        Bytes.toBytes("reg")
    };
    
	public static class SimpleMapper extends Mapper<Object, Text, Text, NullWritable> {
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

			// Country,City,AccentCity,Region,Population,Latitude,Longitude
			String line = value.toString();
			String[] splitted = line.split(",");

			if (splitted[4].length() == 0 || splitted[4].equals("Population"))
				return;
			

			context.write(value, NullWritable.get());
		}
    }

    
    public static class SimpleReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
        Table table;
        int row;
        
        public void old_setup(Context context) throws IOException {
            Connection connection = ConnectionFactory.createConnection(context.getConfiguration());
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            row = 0;
        }

        private Put insertOne(String line, String row) {
            String[] split = line.split(",");
            Put put = new Put(Bytes.toBytes(row));

            put.add(Bytes.toBytes("loc"), Bytes.toBytes("x"), Bytes.toBytes(split[6]));
            put.add(Bytes.toBytes("loc"), Bytes.toBytes("y"), Bytes.toBytes(split[5]));

            put.add(Bytes.toBytes("measure"), Bytes.toBytes("pop"), Bytes.toBytes(split[4]));

            put.add(Bytes.toBytes("name"), Bytes.toBytes("short"), Bytes.toBytes(split[1]));

            put.add(Bytes.toBytes("reg"), Bytes.toBytes("code"), Bytes.toBytes(split[3]));
            put.add(Bytes.toBytes("reg"), Bytes.toBytes("country"), Bytes.toBytes(split[0]));

            return put;
        }

        public void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
                context.write(null, insertOne(key.toString(), String.valueOf(row)));
                this.row += 1;
        }
    }


	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "InsertCities");

		job.setNumReduceTasks(1);
		job.setJarByClass(InsertCities.class);
		
		job.setMapperClass(SimpleMapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/users/robin/worldcitiespop.txt"));

        TableMapReduceUtil.initTableReducerJob(
                "rnavarro-td8",
                SimpleReducer.class,
                job
        );

		return job.waitForCompletion(true)? 0 : 1;
	}
}
