package voting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Step6 {
	public static class step6Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap符号
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitResult[1]), value);//<"Nanhang",整条信息>
		}
	}
	public static class step2CountScoreReducer extends Reducer<Text,Text,IntWritable,IntWritable> {
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 CorrectSituation tt = new CorrectSituation();
			 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//找到南航AT900的正确信息
			 Map<String,Float> correctRate = tt.getCorrectRate();
			 for(Text value : values) {//
				 int predictFlag = 1 ;//预测的情况
				 int actualFlag = 1;//真实的情况
				 
				//-------------------实际的情况---------------
				 String[] saveSplitResult = value.toString().split("/t");//分割
				 if(!saveSplitResult[4].equals(flightInfo[4])) {//判断登机口
					 actualFlag = 0;
				 }
				 if(!saveSplitResult[7].equals(flightInfo[7])) {//判断到达口
					 actualFlag = 0;
				 }
				 //判断出发时间
				 for(int i =2;i<4;i++) {
					  if(!saveSplitResult[i].equals("0")) {//"0"的项不理
						  int timeTest = CountTime(saveSplitResult[i]);
						  int timeTruth = CountTime(flightInfo[i]);
						  if(timeTest!=timeTruth) {
							  actualFlag=0;
						  }
							  
					  }
				 }
				 //到达时间判断
				 for(int i =5;i<7;i++) {
					  if(!saveSplitResult[i].equals("0")) {//"0"的项不理
						  int timeTest = CountTime(saveSplitResult[i]);
						  int timeTruth = CountTime(flightInfo[i]);
						  if(timeTest!=timeTruth) {
							  actualFlag=0;
						  }
							  
					  }
				 }
				 //-------------------预测的情况---------------
				 if(correctRate.get(saveSplitResult[0])<0.5) {//如果正确率小于0.5，被置为错误，即0
					 predictFlag= 0;
				 }
				 
				 context.write(new IntWritable(predictFlag),new IntWritable(actualFlag));
			 }
		 }
    }
	
	//从日期形式，计算出出发时间
	public static int CountTime(String s){
		  System.out.printf("输入字符串：%s\n",s);
		  String actualTime = "0";
		  String[] split = s.split(" ");//把出发时间分片
		  for(String t : split) {//找到包含":"的时间,但是不要括号里面的
			  if(t.contains(":") && !t.contains("(")) {
				  actualTime = t;
				  actualTime = actualTime.replaceAll(" ", "");//先去除空白符号
				  actualTime = actualTime.replaceAll("[a-zA-Z]", "");
				  //TODO something wrong
				  //+、*、|、\等符号在正则表达示中有相应的不同意义
				  actualTime = actualTime.replaceAll("\\*", "");
			  }
		  }
		  System.out.printf("找到的到达时间2  %s\n",actualTime);
		  String[] splitCountTime = actualTime.split(":");
		  int CountTime = Integer.parseInt(splitCountTime[0])*60+Integer.parseInt(splitCountTime[1]);
		  return CountTime;
	}
	
	public static void run() throws IOException {
		//先读入正确的文本
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO 读出信息源的数量，执行十遍，！！！覆盖问题
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/flight_truth/2011-12-27-truth.txt");
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
                	tt.setCorrectInfo(split[0],split);//写信息源的数量
                    System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
		//---------------------读取正确率---------------------------
		Path pathCorrectRate = new Path("hdfs://192.168.126.130:9000/user/findTruth/step5/correctRate.txt");
		if (fs.exists(pathCorrectRate)) {
			System.out.println("Exists!");
			try {
				//此为hadoop读取数据类型
				FSDataInputStream is = fs.open(pathCorrectRate);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	tt.setCorrectRate(split[0],Integer.parseInt(split[1]));//写信息源的数量
                    System.out.println("line="+line);
                    
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
