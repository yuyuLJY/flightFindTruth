package judgeFromExitTruth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import voting.TruthMain;

public class JStep1 {
	public static void run() throws IOException {
		//读取标准的数据
		
		
		
		
		
		
		
		//查找答案
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO 读出信息源的数量，执行十遍，！！！覆盖问题
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/flight_truth/2011-12-01-truth.txt");
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Exists!");
			try {
				//此为hadoop读取数据类型
				FSDataInputStream is = fs.open(pathSourceNumber);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
	}
}
