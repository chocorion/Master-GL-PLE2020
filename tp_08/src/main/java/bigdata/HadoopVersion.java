package bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
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
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopVersion {
    private static final byte[] TABLE_NAME = Byte.toBytes("rnavarro-td8");
    private static final byte[][] FAMILIES = {
        Byte.toBytes("loc"),
        Byte.toBytes("measure"),
        Byte.toBytes("name"),
        Byte.toBytes("reg")
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

    
    public static class SimpleReducer extends Reducer<Text, NullWritable, NullWritable, NullWritable> {
        HTable table;
        int row;
        
        public void setup(Context context) {
            table = new HTable(
                context.getConfiguration(),
                TABLE_NAME
            );

            row = 0;
        }

        public void cleanup(Context context) {
            table.close();
        }

        private void insertOne(String line, String row) {
            String splited = line.split(",");
            Put put = new Put(Bytes.toBytes(row));

            put.add(Bytes.toBytes("loc"), Bytes.toBytes("x"), Bytes.toBytes(splited[6]));
            put.add(Bytes.toBytes("loc"), Bytes.toBytes("y"), Bytes.toBytes(splited[5]));

            put.add(Bytes.toBytes("measure"), Bytes.toBytes("pop"), Bytes.toBytes(splited[4]));

            put.add(Bytes.toBytes("name"), Bytes.toBytes("short"), Bytes.toBytes(splited[1]));

            put.add(Bytes.toBytes("reg"), Bytes.toBytes("code"), Bytes.toBytes(splited[3]));
            put.add(Bytes.toBytes("reg"), Bytes.toBytes("country"), Bytes.toBytes(splited[0]));
        }

        public void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
                insertOne(key.toString(), Sting.valueOf(row));
                this.row += 1;
        }
    }

    
    public static class HadoopInitializer extends Configured implements Tool {

        public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
            }
            
			admin.createTable(table);
		}
        
        public static createTable(Connection connection) {
            try (Admin admin = connection.getAdmin()) {
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                
                for (byte[] family : FAMILIES) {
                    tableDescriptor.addFamily(family);
                }

                createOrOverwrite(admin, tableDescriptor);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        
        public int run(String[] args) throws IOException {
            Connection connection = ConnectionFactory.createConnection(getConf());

            createTable(connection);
            return 0;
        }
    }
	
	public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int hbaseSetupExitCode = ToolRunner(conf, new HadoopInitializer(), args);

		Job job = Job.getInstance(conf, "TP_08");

		job.setNumReduceTasks(1);
		job.setJarByClass(HadoopVersion.class);
		
		job.setMapperClass(SimpleMapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        job.setReducerClass(SimpleReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/users/robin/worldcitiespop.txt"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
