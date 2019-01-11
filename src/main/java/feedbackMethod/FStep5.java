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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FStep5 {
	public static class step5Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			System.out.println(value.toString());
			String[] splitResult = value.toString().split("\t");//tap符号
			System.out.printf("result: %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitResult[0]), new Text(splitResult[1]));//<"Nanhang",整条信息>
		}
	}
	
	//把正确的信息源写成： <"travel",1>
	public static class step5Reducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  FCorrectSituation tt = new FCorrectSituation(); 
			  for(Text value : values) {
					 String[] splitResult = value.toString().split("/t");//tap符号
					 System.out.printf(Arrays.toString(splitResult));
					 if(splitResult[1].equals(splitResult[0])) {//正确的情况
						 tt.setCorrect(tt.getCorrect()+1.0);
					 }else {
						 tt.setWrong(tt.getWrong()+1.0);
					 }
			  }//一个信息源处理完毕
			  System.out.printf("correct:"+tt.getCorrect()+" rate:tt.getCorrect())/(tt.getCorrect()+tt.getWrong())");
			  tt.setperSourceCorrectRate(key.toString(), (tt.getCorrect())/(tt.getCorrect()+tt.getWrong()));
			  //把信息清空
			  tt.setCorrect(0);
			  tt.setWrong(0);
			  
		  }
	}

	
	//输入数据集的文件夹名字，输出数据集文件夹的名字，标准答案的位置，本次读写文件夹的位置
	public static void run(Map<String, String> path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//先读入正确的文本
		System.out.printf("Step5---------------");
		Job job = new Job(FeedbackMain.config(),"Fstep5");

        String input = path.get("Step5Input");
        String output = path.get("Step5Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(FStep5.class);
        job.setMapperClass(step5Mapper.class);
        job.setReducerClass(step5Reducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);//若执行完毕，退出
	}
}
