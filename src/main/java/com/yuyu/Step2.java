package com.yuyu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
public class Step2 {
	//按照航班把信息分类
	public static class step2CountScoreMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap符号
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"Nanhang",整条信息>
		}
	}
	
	public static class step2CountScoreReducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //定义输出值始终是1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  Map<String,Integer> departureGate = new HashMap<String,Integer>();
			  Map<String,Integer> arriveGate = new HashMap<String,Integer>();
			  //存好数据
			  //用来存这次的信息
			  ArrayList<String[]> flightInfo = new ArrayList<String[]>();
			  for(Text value : values) {//e.g 南航AMT-100
				  //先计数
				  //出发登机口
				  //System.out.printf("%s\n",value);
				  String[] saveSplitResult = value.toString().split("/t");
				  flightInfo.add(saveSplitResult);
				  System.out.printf("%s\n",Arrays.toString(saveSplitResult));
				  String departureGateName = saveSplitResult[4].replaceAll(" ", "");//标准化出发口
				  if(departureGate.containsKey(departureGateName)==false) {//还没有包含在里边
					  departureGate.put(departureGateName, 1);
				  }else {//出口信息已经包含了
					  departureGate.put(departureGateName,(departureGate.get(departureGateName)+1));
				  }
				  //到达登机口
				  String arriveGateName = saveSplitResult[7].replaceAll(" ", "");//标准化出发口
				  if(arriveGate.containsKey(arriveGateName)==false) {//还没有包含在里边
					  arriveGate.put(arriveGateName, 1);
				  }else {//出口信息已经包含了
					  arriveGate.put(arriveGateName,arriveGate.get(arriveGateName)+1);
				  }
			  }
			  System.out.println("查看出发集合情况");
			  for(String s : departureGate.keySet()) {
				  System.out.printf("%s %d\n",s,departureGate.get(s));  
			  }
			  //选出key最大值
			  int maxDepartureGate = 0;
			  String maxDepartureGateKey = "";
			  int maxArriveGate = 0;
			  String maxArriveGateKey = "";
			  for(String s : departureGate.keySet()) {
				  if(departureGate.get(s)>maxDepartureGate && !s.equals("0")) {
					  maxDepartureGate = departureGate.get(s);
					  maxDepartureGateKey = s;
				  }
			  }
			  for(String s : arriveGate.keySet()) {
				  if(arriveGate.get(s)>maxArriveGate && !s.equals("0")) {//并且不能是0
					  maxArriveGate = arriveGate.get(s);
					  maxArriveGateKey = s;
				  }
			  }
			  //给对应的信息源赋值
			  for(String[] saveSplitResult : flightInfo) {
				  //String[] saveSplitResult = .toString().split("/t");
				  //System.out.printf("%s\n",Arrays.toString(saveSplitResult));
				  //System.out.printf("最大出发点： %s 信息：%s\n",maxDepartureGateKey,(saveSplitResult[4].replaceAll(" ", "")));
				  if((saveSplitResult[4].replaceAll(" ", "")).equals(maxDepartureGateKey)) {
					  System.out.printf("被选中的信息源：%s\n",saveSplitResult[0]);
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  if((saveSplitResult[7].replaceAll(" ", "")).equals(maxArriveGateKey)) {
					  context.write(new Text(saveSplitResult[0]),one);
				  }
			  }
			  //System.out.printf("reduce\n");
		  }
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(TruthMain.config(),"step2");
        String input = path.get("Step2Input");
        String output = path.get("Step2Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(Step2.class);
        job.setMapperClass(step2CountScoreMapper.class);
        job.setReducerClass(step2CountScoreReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
