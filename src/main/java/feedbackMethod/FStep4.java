package feedbackMethod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//统计正确性
public class FStep4 {
	//按照航班把信息分类
	public static class step4Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap符号
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"Nanhang",整条信息>
		}
	}
	
	//把正确的信息源写成： <"travel",1>
	public static class step4Reducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				 FCorrectSituation tt = new FCorrectSituation();
				 if(tt.getCorrecInfo().containsKey(key.toString())) {//答案里边有
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//找到南航AT900的正确信息
				 //------------------验证flightInfo-----------------------------
				 System.out.printf("Step4正确情况： %s\n",Arrays.toString(flightInfo));
				 for(Text value : values) {
					//-------------------实际的情况---------------
					 String[] saveSplitResult = value.toString().split("/t");//分割
					 System.out.printf("信息源 %s\n",Arrays.toString(saveSplitResult));
					 
					 //------------------判断登机口---------------
					 int cateActualFlag1 = 1;//真实的情况
					 int catePredictFlag1=1;
					 int cateActualFlag2 = 1;//真实的情况
					 int catePredictFlag2=1;
					 if(!saveSplitResult[4].equals("0") && !flightInfo[3].equals("0")) {
						 if(!saveSplitResult[4].equals(flightInfo[3])) {//判断登机口
							 cateActualFlag1 = 0;
						 }
						 if(tt.getRealityI().get(saveSplitResult[0])<0.5) {
							 catePredictFlag1=0;
						 }
						 String s = String.valueOf(catePredictFlag1)+"/t"+String.valueOf(cateActualFlag1);
						 context.write(new Text(saveSplitResult[0]),new Text(s)); 
					 }
					 
					 if(!saveSplitResult[7].equals("0") && !flightInfo[6].equals("0")) {
						 if(!saveSplitResult[7].equals(flightInfo[6])) {//判断登机口
							 cateActualFlag2 = 0;
						 }
						 if(tt.getRealityI().get(saveSplitResult[0])<0.5) {
							 catePredictFlag2=0;
						 }
						 String s = String.valueOf(catePredictFlag2)+"/t"+String.valueOf(cateActualFlag2);
						 context.write(new Text(saveSplitResult[0]),new Text(s)); 
					 }

					 //------------------判断出发时间-----------------
					 for(int i =2;i<4;i++) {
						 int conActualFlag1 = 1;//出发的真实情况
						 int conPredictFlag1=1;
						 if(!saveSplitResult[i].equals("0")) {//"0"的项不理
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if( timeTruth !=0 && timeTest!=timeTruth) {//如果正确信息缺失则不比较
								  conActualFlag1=0;
							  }
							  if(tt.getRealityJ().get(saveSplitResult[0])<0.5) {
								  conPredictFlag1=0;  
							  }
							  String s = String.valueOf(catePredictFlag2)+"/t"+String.valueOf(cateActualFlag2);
							  context.write(new Text(saveSplitResult[0]),new Text(s));   
						 }
					 }
					 //到达时间判断
					 for(int i =5;i<7;i++) {
						 int conActualFlag2 = 1;//真实的情况
						 int conPredictFlag2=1;
						  if(!saveSplitResult[i].equals("0")) {//"0"的项不理
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if( timeTruth !=0 && timeTest!=timeTruth) {//如果正确信息缺失则不比较
								  conActualFlag2=0;
							  }
							  if(tt.getRealityJ().get(saveSplitResult[0])<0.5) {
								  conPredictFlag2=0;  
							  }
							  String s = String.valueOf(catePredictFlag2)+"/t"+String.valueOf(cateActualFlag2);
							  context.write(new Text(saveSplitResult[0]),new Text(s));   
								  
						  }
					 }
				 }
			}
		  }
	}
	
	//从日期形式，计算出出发时间
	public static int CountTime(String s){
		  //System.out.printf("输入字符串：%s\n",s);
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
		  //System.out.printf("找到的到达时间  %s\n",actualTime);
		  int CountTime =0;
		  if(actualTime.contains(":")) {
			  String[] splitCountTime = actualTime.split(":");
			  CountTime = Integer.parseInt(splitCountTime[0])*60+Integer.parseInt(splitCountTime[1]);  
		  }
		  return CountTime;
	}
	
	//输入数据集的文件夹名字，输出数据集文件夹的名字，标准答案的位置，本次读写文件夹的位置
	public static void run(Map<String, String> path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//先读入正确的文本
		FCorrectSituation tt = new FCorrectSituation();
		
		Configuration conf=FeedbackMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.131:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO 读出信息源的标准情况，执行十遍，！！！覆盖问题
		//Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/standard_truth_flight/part-r-00000");
		//System.out.println("读取每月标准答案："+pathTruth.get(outputName)+"/part-r-00000");
		//Path pathSourceNumber = new Path(pathTruth.get(outputName)+"/part-r-00000");
		Path pathSourceNumber = new Path("hdfs://192.168.126.131:9000/user/findTruth/feedback/data/TestTruth1211/part-r-00000");
		tt.getCorrecInfo().clear();//!!!!!统计每个月的时候，要清除
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Step4:Exists!");
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
                	String[] splitValue = split[1].split("/t"); 
                	tt.setCorrectInfo(split[0],splitValue);//写信息源的情况
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("Step4不存在");
		}
		
		//----------------------------验证读入正确
		//Map<String,String[]> correcInfo = tt.getCorrecInfo();
		//System.out.println("验证正确信息");
		//for(String s : correcInfo.keySet()) {
			//System.out.printf("%s %s\n",s,Arrays.toString(correcInfo.get(s)));
		//}
		
		Job job = new Job(FeedbackMain.config(),"Fstep4");

        String input = path.get("Step4Input");
        String output = path.get("Step4Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(FStep4.class);
        job.setMapperClass(step4Mapper.class);
        job.setReducerClass(step4Reducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);//若执行完毕，退出
		
	}
}
