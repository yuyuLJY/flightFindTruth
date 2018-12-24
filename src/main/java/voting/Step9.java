package voting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Step9 {
	public static void run(Map<String, String> path) throws IOException {
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		
		//TODO 读出信息源的数量，执行十遍，！！！覆盖问题
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/Voting/step8/part-r-00000");
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
                int wrongNumber = 0;
                int correctNumber = 0;
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                    System.out.printf("%s: \n",Arrays.toString(split));//[0, 0, 458]
                    if(split[0].equals(split[1])) {//正确的
                    	correctNumber += Integer.parseInt(split[2]);
                    }else {
                    	wrongNumber += Integer.parseInt(split[2]);
                    }
                }
                //计算正确率
                float wholeCorrectRate = (float)correctNumber/((float)(wrongNumber+correctNumber));
                System.out.printf("样本集的正确率为：%f",wholeCorrectRate);
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
	}
}
