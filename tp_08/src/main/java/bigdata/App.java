import org.apache.hadoop.util.ToolRunner;

public class App {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();

        int resInit   = ToolRunner.run(conf, new InitTable(), args);
        int resInsert = ToolRunner.run(conf, new InserCities(), args);

        System.exit(resInsert);
    }    
}
