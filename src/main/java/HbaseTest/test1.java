package HbaseTest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class test1 {

    @Test
    public void put() throws Exception {
        //创建conf对象    会加载你项目资源文件下的两个XML文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
        //通过连接工厂创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //通过连接查询tableName对象    表名
        TableName tname = TableName.valueOf("ns1:t1");
        //获得table
        Table table = conn.getTable(tname);
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes("row1");  //rowkey名
        //创建put对象
        Put put = new Put(rowid);
        byte[] f1 = Bytes.toBytes("f1");  //列簇名
        byte[] id = Bytes.toBytes("id"); //列名
        byte[] value = Bytes.toBytes(102); //设置值
        put.addColumn(f1, id, value);
        //执行插入
        table.put(put);
    }
    @Test
    public void createTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建表名对象
//        TableName tableName = TableName.valueOf("ns2:t2");
        TableName tableName = TableName.valueOf("test");
        //创建表描述符对象
        HTableDescriptor tbl = new HTableDescriptor(tableName);
        //创建列族描述符
        HColumnDescriptor col = new HColumnDescriptor("f1" );
        tbl.addFamily(col);

        admin.createTable(tbl);
        System.out.println("over");
    }

    @Test
    public void createNameSpace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建名字空间描述符
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns2").build();
        admin.createNamespace(nsd);
        //列举出所有的工作空间
        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for(NamespaceDescriptor n : ns){
            System.out.println(n.getName());
        }
    }


}
