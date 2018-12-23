package voting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Step5 {
	public static void run(Map<String, String> path) throws IOException {
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO 读出信息源的数量，执行十遍，！！！覆盖问题
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/step4/part-r-00000");
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
                	tt.setSourceNumber(split[0], Integer.parseInt(split[1]));//写信息源的数量
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
		
		//TODO 读出信息源正确的数量，执行十遍，！！！考虑覆盖问题
		Path pathCorrect = new Path("hdfs://192.168.126.130:9000/user/findTruth/step3/part-r-00000");
		if (fs.exists(pathCorrect)) {
			System.out.println("Exists!");
			try {
				//此为hadoop读取数据类型
				FSDataInputStream is = fs.open(pathCorrect);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	tt.setCorrectNumber(split[0], Integer.parseInt(split[1]));//写信息源的数量
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
		
		
		//------------------------验证是否正确------------------------
		Map<String,Integer> sourceNumber = tt.getSourceNumber();
		//System.out.println("验证信息源的个数");
		//for(String s : sourceNumber.keySet()) {
			//System.out.printf("%s %d\n",s,sourceNumber.get(s));
		//}
		
		Map<String,Integer> sourceCorrectNumber  = tt.getCorrectNumber();
		//System.out.println("验证正确的个数");
		//for(String s : sourceCorrectNumber.keySet()) {
			//System.out.printf("%s %d\n",s,sourceCorrectNumber.get(s));
		//}
		
		//-----------------------------计算正确率-------------------------
		for(String s : sourceNumber.keySet()) {
			float correctRate = 0;
			correctRate = (float)sourceCorrectNumber.get(s)/(float)(sourceNumber.get(s)*4);
			tt.setCorrectRate(s, correctRate);
		}
		
		//---------------------------验证正确率-------------------
		Map<String,Float> correctRateMap  = tt.getCorrectRate();
		System.out.println("验证准确率");
		for(String s : correctRateMap.keySet()) {
			System.out.printf("%s	%f\n",s,correctRateMap.get(s));
		}
		
		//-----------------将准确率写入hdfs文件------------
		Path CorrectRatePath = new Path("hdfs://192.168.126.130:9000/user/findTruth/step5/1");
		FSDataOutputStream outStream=fs.create(CorrectRatePath);
		
	}
}
