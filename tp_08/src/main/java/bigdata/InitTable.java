package bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class InitTable extends Configured implements Tool {
    private static final byte[] TABLE_NAME = Bytes.toBytes("rnavarro-td8");
    private static final byte[][] FAMILIES = {
            Bytes.toBytes("loc"),
            Bytes.toBytes("measure"),
            Bytes.toBytes("name"),
            Bytes.toBytes("reg")
    };

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        
        admin.createTable(table);
    }
    
    public static void createTable(Connection connection) {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            
            for (byte[] family : FAMILIES) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
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