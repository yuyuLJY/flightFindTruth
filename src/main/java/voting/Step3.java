package voting;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;

public class Step3 {
	public static class step3CountSourceMapper extends Mapper<Object, Text, Text, IntWritable>{
		IntWritable one = new IntWritable(1);  //定义输出值始终是1
		Text text = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap符号
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			text.set(splitResult[0]);
			context.write(text, one);//<"Nanhang",整条信息>
			//System.out.println("map finish");
		}
	}
	
	//!!!!!开始的时候忘记写static，所以Reducer一直不被调用
	public static class step3CountSourceReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		  IntWritable result = new IntWritable();
		  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			  int sum = 0;
			  for (IntWritable val : values) {      
				  sum += val.get();    
			  }    
			  result.set(sum);    
			  //System.out.printf("key:%s sum:%d\n",key,sum);
			  context.write(key, result);//每个IntWritable都是对应的key的Int的集合，所以key-value的放进去  
		  } 
	}
	
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(TruthMain.config(),"step3");
        String input = path.get("Step3Input");
        String output = path.get("Step3Output");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(Step3.class);
        job.setMapperClass(step3CountSourceMapper.class);
        job.setReducerClass(step3CountSourceReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
