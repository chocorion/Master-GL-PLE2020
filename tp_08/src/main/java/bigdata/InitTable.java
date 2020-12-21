public static class InitTable extends Configured implements Tool {

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