package com.yuyu;

import java.io.IOException;
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
			System.out.printf("map %s\n",value);
			String[] splitResult = value.toString().split("\t");
			context.write(new Text(splitResult[1]), value);//<"Nanhang",整条信息>
		}
	}
	
	public static class step2CountScoreReducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //定义输出值始终是1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  Map<String,Integer> departureGate = new HashMap<String,Integer>();
			  Map<String,Integer> arriveGate = new HashMap<String,Integer>();
			  //存好数据
			  for(Text value : values) {//e.g 南航AMT-100
				  //先计数
				  //到达登机口
				  System.out.printf("%s\n",value);
				  String[] saveSplitResult = value.toString().split("\t");
				  if(departureGate.containsKey(saveSplitResult[4])==false) {//还没有包含在里边
					  departureGate.put(saveSplitResult[4], 1);
				  }else {//出口信息已经包含了
					  departureGate.put(saveSplitResult[4],(departureGate.get(saveSplitResult[4])+1));
				  }
				  //出发登机口
				  if(arriveGate.containsKey(saveSplitResult[7])==false) {//还没有包含在里边
					  arriveGate.put(saveSplitResult[7], 1);
				  }else {//出口信息已经包含了
					  arriveGate.put(saveSplitResult[7],arriveGate.get(saveSplitResult[7])+1);
				  }
			  }
			  
			  //选出key最大值
			  int maxDepartureGate = 0;
			  String maxDepartureGateKey = "";
			  int maxArriveGate = 0;
			  String maxArriveGateKey = "";
			  for(String s : departureGate.keySet()) {
				  if(departureGate.get(s)>maxDepartureGate) {
					  maxDepartureGate = departureGate.get(s);
					  maxDepartureGateKey = s;
				  }
			  }
			  for(String s : arriveGate.keySet()) {
				  if(arriveGate.get(s)>maxArriveGate) {
					  maxArriveGate = arriveGate.get(s);
					  maxArriveGateKey = s;
				  }
			  }
			  //给对应的信息源赋值
			  for(Text value : values) {
				  String[] saveSplitResult = value.toString().split("\t");
				  if(saveSplitResult[4].equals(maxDepartureGateKey)) {
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  if(saveSplitResult[7].equals(maxArriveGateKey)) {
					  context.write(new Text(saveSplitResult[0]),one);
				  }
			  }
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
