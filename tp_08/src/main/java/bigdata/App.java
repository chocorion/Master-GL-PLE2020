package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

public class App {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int res = 0;

        res &= ToolRunner.run(conf, new InitTable(), args);
        res &= ToolRunner.run(conf, new InsertCities(), args);

        System.exit(res);
    }    
}
