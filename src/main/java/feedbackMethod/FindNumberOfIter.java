package feedbackMethod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindNumberOfIter {
	public static class stepIterMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap符号
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"MP190",整条信息>
		}
	}
	
	public static class stepIterReducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //定义输出值始终是1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			FCorrectSituation tt = new FCorrectSituation();
			if(tt.getCorrecInfo().containsKey(key.toString())) {//答案里边有
				tt.setIter(tt.getIter()+1);
			}else {
				System.out.println("没有答案：  "+key.toString());
			}
		  }
	}
	
	public static void run(Map<String, String> path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		FCorrectSituation tt = new FCorrectSituation();
		
		Configuration conf=FeedbackMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.131:9000");
		FileSystem fs = FileSystem.get(conf);
		
		Path pathSourceNumber = new Path("hdfs://192.168.126.131:9000/user/findTruth/feedback/truth1201/part-r-00000");
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
		
		Job job = new Job(FeedbackMain.config(),"Find");
        String input = path.get("StepIterInput");
        String output = path.get("StepIterOutput");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(FindNumberOfIter.class);
        job.setMapperClass(stepIterMapper.class);
        job.setReducerClass(stepIterReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);//若执行完毕，退出
		
	}
}
