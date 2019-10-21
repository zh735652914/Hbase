package com.smh.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSClient {

	public static void main(String args[]) throws IOException {
		
		Configuration conf=new Configuration();
		// 1.获取hdfs客户端对象
		conf.set("fs.defaultFS","hdfs://localhost:8020");
		//FileSystem fs=FileSystem.get(new URI("hdfs://localhost:9000"),conf,"username");
		FileSystem fs=FileSystem.get(conf);
		// 2 在hdfs上创建路径
		fs.mkdirs(new Path("/0403/dashen"));
		
		//3.关闭
		fs.close();
		
		System.out.println("over");
	}
	// 文件上传
	@Test
	public void testCopyFromeLocalFile() throws IOException {
		// 1 获取fs对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:8020");
		conf.set("dfs.replication", "2");
		FileSystem fs=FileSystem.get(conf);
		
		// 2执行上传api
		fs.copyFromLocalFile(new Path("D:\\program\\readme.txt"), new Path("/0403/dashen/dahuahua.txt"));
		
		
		
		// 3 关闭资源
		
		fs.close();
		System.out.println("testCopyFromeLocalFile over");
	}
	
	//文件delete
	@Test
	public void testDetele() throws IOException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		FileSystem fs=FileSystem.get(conf);
		
		fs.delete(new Path("/0403/dashen/readme.txt"));
		fs.close();
		System.out.println("delete over");
	}
	//文件查看
	@Test
	public void testList() throws FileNotFoundException, IllegalArgumentException, IOException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		FileSystem fs=FileSystem.get(conf);
		
		RemoteIterator<LocatedFileStatus> listFile=fs.listFiles(new Path("/"), true);
		
		while(listFile.hasNext()) {
			LocatedFileStatus fileStatus=listFile.next();
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getPermission());
			//System.out.println(fileStatus.getBlockLocations());
			BlockLocation[] bs=fileStatus.getBlockLocations();
			for(BlockLocation b:bs) {
				String []host=b.getHosts();
				for(String s:host){
					System.out.println(s);
				}
			}
			System.out.println("---------------------");
		}
	}

	
}
