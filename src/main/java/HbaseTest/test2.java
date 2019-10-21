package HbaseTest;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

//从服务器读取数据

public class test2 {

    @Test
    public void getData() throws Exception {
        String path = "/home/zyj/LaboratoryProject/22data_process/DATA";
//        String path = "/home/zyj/LaboratoryProject/22data_process/data_col11-6811/201809191600000430col.txt";
        String hostName = "10.109.29.21";
        int port = 22;
        String username = "zyj";
        String password = "1210";
        Connection conn = new Connection(hostName, port);
        boolean isconn = false;
        String FirstFile = "";
        try {
            // 连接到主机
            conn.connect();
            // 使用用户名和密码校验
            isconn = conn.authenticateWithPassword(username, password);
            if (!isconn) {
                System.out.println("用户名称或者是密码不正确");
            } else {
                System.out.println("Established connection!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (isconn) {
            Session ss = null;
            try {
                ss = conn.openSession();
                ss.execCommand("ls ".concat(path));
                InputStream is = new StreamGobbler(ss.getStdout());
                BufferedReader brs = new BufferedReader(new InputStreamReader(is));
                String line = "";

                FirstFile = brs.readLine();
                line = line + FirstFile;

                while (true) {
                    String lineInfo = brs.readLine();
                    if (lineInfo != null) {
                        line = line + lineInfo;
                    } else {
                        break;
                    }
                }
                brs.close();
                if (line != null && line.length() > 0 && line.startsWith("-")) {
                    System.out.println("~~~");
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("文件为空");
            } finally {
                // 连接的Session和Connection对象都需要关闭
                if (ss != null) {
                    ss.close();
                }
            }
        }

        if (isconn) {
            Session ss = null;
            try {
                ss = conn.openSession();
                ss.execCommand("head -1 ".concat(path).concat("/").concat(FirstFile));
                InputStream is = new StreamGobbler(ss.getStdout());
                BufferedReader brs = new BufferedReader(new InputStreamReader(is));

//                System.out.println("A readLine is : ");
//                System.out.println(brs.readLine());

                while (true) {
                    String line = brs.readLine();
                    if (line == null) {
                        break;
                    } else {
                        System.out.println(line);
                    }
                }
                brs.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("文件为空");
            } finally {
                // 连接的Session和Connection对象都需要关闭
                if (ss != null) {
                    ss.close();
                }
                if (isconn) {
                    conn.close();
                }
            }
        }


    }

    //有问题的测试
    @Test
    public void putData() throws Exception {
        String path = "/home/zyj/LaboratoryProject/22data_process/DATA";
//        String path = "/home/zyj/LaboratoryProject/22data_process/data_col11-6811/201809191600000430col.txt";
        String hostName = "10.109.29.21";
        int port = 22;
        String username = "zyj";
        String password = "1210";
        Connection conn0 = new Connection(hostName, port);
        boolean isconn = false;
        String FirstFile = "";
        try {
            // 连接到主机
            conn0.connect();
            // 使用用户名和密码校验
            isconn = conn0.authenticateWithPassword(username, password);
            if (!isconn) {
                System.out.println("用户名称或者是密码不正确");
            } else {
                System.out.println("Established connection!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        String line = "";
        if (isconn) {
            Session ss = null;
            try {
                ss = conn0.openSession();
                ss.execCommand("ls ".concat(path));
                InputStream is0 = new StreamGobbler(ss.getStdout());
                BufferedReader brs0 = new BufferedReader(new InputStreamReader(is0));
                FirstFile = brs0.readLine();//读取文件名

                ss.execCommand("head -1 ".concat(path).concat("/").concat(FirstFile));
                InputStream is = new StreamGobbler(ss.getStdout());
                BufferedReader brs = new BufferedReader(new InputStreamReader(is));

//                System.out.println("A readLine is : ");
//                System.out.println(brs.readLine());

                while (true) {
                    line = brs.readLine();
                    if (line == null) {
                        break;
                    } else {
                        System.out.println(line);
                    }
                }
                brs.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("文件为空");
            } finally {
                // 连接的Session和Connection对象都需要关闭
                if (ss != null) {
                    ss.close();
                }
                if (isconn) {
                    conn0.close();
                }
            }
        }


        //向hbase写入数据，数据在变量line中
//        String[] data = new String[line.length()];
        String[] data = line.split(" ");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
        //通过连接工厂创建连接对象
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(conf);
        //通过连接查询tableName对象    表名
        TableName tname = TableName.valueOf("22data");
        //获得table
        Table table = conn.getTable(tname);
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes(data[1]);  //rowkey名
        //创建put对象
        Put put = new Put(rowid);

        byte[] time = Bytes.toBytes("time");  //列簇名
        byte[] id = Bytes.toBytes("test1"); //列名
        byte[] value = Bytes.toBytes(data[1]); //设置值
        put.addColumn(time, id, value);
        table.put(put);//执行插入

        put.addColumn(Bytes.toBytes("longitude"), Bytes.toBytes("test1"), Bytes.toBytes(data[2]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("flag"), Bytes.toBytes("test1"), Bytes.toBytes(data[4]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("serialNumserialNum"), Bytes.toBytes("test1"), Bytes.toBytes(data[5]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("startingFrequency"), Bytes.toBytes("test1"), Bytes.toBytes(data[6]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("terminationFrequency"), Bytes.toBytes("test1"), Bytes.toBytes(data[7]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("interval"), Bytes.toBytes("test1"), Bytes.toBytes(data[8]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("nums"), Bytes.toBytes("test1"), Bytes.toBytes(data[9]));
        table.put(put);//执行插入

        for (int i = 10; i < data.length; i++) {
            put.addColumn(Bytes.toBytes("fieldStrength"), Bytes.toBytes(i), Bytes.toBytes(data[i]));
            table.put(put);//执行插入
        }
    }

    @Test
    public void put() throws Exception {
        //创建conf对象    会加载你项目资源文件下的两个XML文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
        //通过连接工厂创建连接对象
//        Connection conn = ConnectionFactory.createConnection(conf);
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(conf);
        //通过连接查询tableName对象    表名
        TableName tname = TableName.valueOf("test1");
        //获得table
        Table table = conn.getTable(tname);
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes("row1");  //rowkey名
        //创建put对象
        Put put = new Put(rowid);
        byte[] f1 = Bytes.toBytes("f1");  //列簇名
        byte[] id = Bytes.toBytes("id"); //列名
        byte[] value = Bytes.toBytes("123456"); //设置值
        put.addColumn(f1, id, value);
        //执行插入
        table.put(put);
    }

    @Test
    public void createTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
//        Connection conn = ConnectionFactory.createConnection(conf);
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建表名对象
//        TableName tableName = TableName.valueOf("ns2:t2");
        TableName tableName = TableName.valueOf("test1");
        //创建表描述符对象
        HTableDescriptor tbl = new HTableDescriptor(tableName);
        //创建列族描述符
        HColumnDescriptor col = new HColumnDescriptor("f1");
        tbl.addFamily(col);

        admin.createTable(tbl);
        System.out.println("over");
    }

    @Test
    public void createNameSpace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
//        Connection conn = ConnectionFactory.createConnection(conf);
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建名字空间描述符
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns2").build();
        admin.createNamespace(nsd);
        //列举出所有的工作空间
        NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor n : ns) {
            System.out.println(n.getName());
        }
    }

}
