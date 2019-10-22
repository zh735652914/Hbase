package HbaseTest;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class test3 {
    public String GetData() throws Exception {
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

        String data = "";
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
                        data = line;
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
        return data;
    }

    public void PutData(String[] data) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.109.29.21");
        //通过连接工厂创建连接对象
        org.apache.hadoop.hbase.client.Connection conn = ConnectionFactory.createConnection(conf);
        //通过连接查询tableName对象    表名
        TableName tname = TableName.valueOf("data22");
        //获得table
        Table table = conn.getTable(tname);
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes(data[1]);  //rowkey名
        //创建put对象
        Put put = new Put(rowid);

        byte[] time = Bytes.toBytes("infor");  //列簇名
        byte[] id = Bytes.toBytes("time"); //列名
        byte[] value = Bytes.toBytes(data[1]); //设置值
        put.addColumn(time, id, value);
        table.put(put);//执行插入

        put.addColumn(Bytes.toBytes("infor"), Bytes.toBytes("longitude"), Bytes.toBytes(data[2]));
        table.put(put);//执行插入
        put.addColumn(Bytes.toBytes("infor"), Bytes.toBytes("latitude"), Bytes.toBytes(data[3]));
        table.put(put);//执行插入

//        put.addColumn(Bytes.toBytes("flag"), Bytes.toBytes("test1"), Bytes.toBytes(data[4]));
//        table.put(put);//执行插入
//        put.addColumn(Bytes.toBytes("serialNum"), Bytes.toBytes("test1"), Bytes.toBytes(data[5]));
//        table.put(put);//执行插入
//        put.addColumn(Bytes.toBytes("startingFrequency"), Bytes.toBytes("test1"), Bytes.toBytes(data[6]));
//        table.put(put);//执行插入
//        put.addColumn(Bytes.toBytes("terminationFrequency"), Bytes.toBytes("test1"), Bytes.toBytes(data[7]));
//        table.put(put);//执行插入
//        put.addColumn(Bytes.toBytes("interval"), Bytes.toBytes("test1"), Bytes.toBytes(data[8]));
//        table.put(put);//执行插入
//        put.addColumn(Bytes.toBytes("nums"), Bytes.toBytes("test1"), Bytes.toBytes(data[9]));
//        table.put(put);//执行插入

        //速度慢，但是正确
//        for (int i = 10; i < data.length; i++) {
//            put.addColumn(Bytes.toBytes("fieldStrength"), Bytes.toBytes(String.valueOf(i)), Bytes.toBytes(data[i]));
//            table.put(put);//执行插入
//            System.out.println("This is " + i + "!");
//        }
//        System.out.println("Over!");
        table.close();

        //正确，且快速
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("zookeeper.znode.parent", "127.0.0.1");
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "10.109.29.21");
        BufferedMutator table2 = null;
        BufferedMutatorParams htConfig = new BufferedMutatorParams(TableName.valueOf("data22")).writeBufferSize(10 * 1024 * 1024);
        org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(configuration);
        table2 = connection.getBufferedMutator(htConfig);
        int count = 0;
        List<Put> puts = new ArrayList<Put>(2048);

        int n = 0;
        String band = "";
        for (int i = 4; i < data.length; i++) {
            if (data[i].equals("1111.0")) {
                n = 0;
                if ("1.0".equals(data[i + 1])) {
                    band = "bandOne";
                } else if ("2.0".equals(data[i + 1])) {
                    band = "bandTwo";
                } else if ("3.0".equals(data[i + 1])) {
                    band = "bandThree";
                } else {
                    band = "bandFour";
                }
                put.addColumn(Bytes.toBytes(band), Bytes.toBytes("serialNum"), Bytes.toBytes(data[++i]));
                put.addColumn(Bytes.toBytes(band), Bytes.toBytes("startingFrequency"), Bytes.toBytes(data[++i]));
                put.addColumn(Bytes.toBytes(band), Bytes.toBytes("terminationFrequency"), Bytes.toBytes(data[++i]));
                put.addColumn(Bytes.toBytes(band), Bytes.toBytes("interval"), Bytes.toBytes(data[++i]));
                put.addColumn(Bytes.toBytes(band), Bytes.toBytes("nums"), Bytes.toBytes(data[++i]));
                i++;
            }
            String field = "fieldStrength" + n;
            Put p = new Put(Bytes.toBytes(data[1]));
            p.addColumn(Bytes.toBytes(band), Bytes.toBytes(field), Bytes.toBytes(data[i]));
            puts.add(p);
            n++;
            count++;
            if (count % 3000 == 0) {
                System.out.println("count:" + count);
                table2.mutate(puts);
                puts = new ArrayList<Put>(2048);
            }
        }
        // 提交最后的内容
        System.out.println("Total count:" + count);
        table2.mutate(puts);
    }

    public static void main(String[] args) throws Exception {
        test3 TS = new test3();
        String line = TS.GetData();
        String[] data = line.split(" ");
        TS.PutData(data);
    }
}
