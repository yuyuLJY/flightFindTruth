package judgeFromExitTruth;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 读入：每个月的标准的航班信息
 * 读出：把正确的信息源写成： <"travel",1>，写进"step/i"中
 * 先把正确的标准Truth读进correctInfo里边,然后用标准答案跟训练集对比
 * */
public class JStep4 {
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
	public static class step4Reducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //定义输出值始终是1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				 CorrectSituation tt = new CorrectSituation();
				 if(tt.getCorrecInfo().containsKey(key.toString())) {//答案里边有
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//找到南航AT900的正确信息
				 //------------------验证flightInfo-----------------------------
				 System.out.printf("正确情况： %s\n",Arrays.toString(flightInfo));
				 for(Text value : values) {
					 int actualFlag = 1;//真实的情况
					 
					//-------------------实际的情况---------------
					 String[] saveSplitResult = value.toString().split("/t");//分割
					 System.out.printf("信息源 %s\n",Arrays.toString(saveSplitResult));
					 if(!saveSplitResult[4].equals("0") && !flightInfo[3].equals("0")&& !saveSplitResult[4].equals(flightInfo[3])) {//判断登机口
						 actualFlag = 0;
					 }
					 if(!saveSplitResult[7].equals("0") && !flightInfo[6].equals("0")&&!saveSplitResult[7].equals(flightInfo[6])) {//判断到达口
						 actualFlag = 0;
					 }
					 //判断出发时间
					 for(int i =2;i<4;i++) {
						  if(!saveSplitResult[i].equals("0")) {//"0"的项不理
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if( timeTruth !=0 && timeTest!=timeTruth) {//如果正确信息缺失则不比较
								  actualFlag=0;
							  }
								  
						  }
					 }
					 //到达时间判断
					 for(int i =5;i<7;i++) {
						  if(!saveSplitResult[i].equals("0")) {//"0"的项不理
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if(timeTruth !=0 && timeTest!=timeTruth) {
								  actualFlag=0;
							  }
								  
						  }
					 }
					 System.out.println("判断结果："+actualFlag);
					 //---------------如果本条训练集正确，则把数据源加进去---------------
					 if(actualFlag==1) {
						 context.write(new Text(saveSplitResult[0]),one); 
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
	public static void run(String inputName,String outputName,Map<String, String> pathTruth,Map<String, String> step4Path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//先读入正确的文本
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=JTruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO 读出信息源的标准情况，执行十遍，！！！覆盖问题
		//Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/standard_truth_flight/part-r-00000");
		System.out.println("读取每月标准答案："+pathTruth.get(outputName)+"/part-r-00000");
		Path pathSourceNumber = new Path(pathTruth.get(outputName)+"/part-r-00000");
		tt.getCorrecInfo().clear();//!!!!!统计每个月的时候，要清除
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
                	String[] splitValue = split[1].split("/t"); 
                	tt.setCorrectInfo(split[0],splitValue);//写信息源的情况
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
		
		//----------------------------验证读入正确
		//Map<String,String[]> correcInfo = tt.getCorrecInfo();
		//System.out.println("验证正确信息");
		//for(String s : correcInfo.keySet()) {
			//System.out.printf("%s %s\n",s,Arrays.toString(correcInfo.get(s)));
		//}
		
		Job job = new Job(JTruthMain.config(),"step4");
		System.out.println("读入MR的数据集："+step4Path.get(inputName));
		System.out.println("写进MR的数据集："+step4Path.get(outputName));
        String input = step4Path.get(inputName);
        String output = step4Path.get(outputName);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(JStep4.class);
        job.setMapperClass(step4Mapper.class);
        job.setReducerClass(step4Reducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);//若执行完毕，退出
		
	}
}
