package judgeFromExitTruth;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JStep5 {
	public static class step5CountSourceMapper extends Mapper<Object, Text, Text, IntWritable>{
		IntWritable one = new IntWritable(1);  //�������ֵʼ����1
		Text text = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap����
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			text.set(splitResult[0]);
			context.write(text, one);//<"Nanhang",������Ϣ>
			//System.out.println("map finish");
		}
	}
	
	//!!!!!��ʼ��ʱ������дstatic������Reducerһֱ��������
	public static class step5CountSourceReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		  IntWritable result = new IntWritable();
		  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			  int sum = 0;
			  for (IntWritable val : values) {      
				  sum += val.get();    
			  }    
			  result.set(sum);    
			  //System.out.printf("key:%s sum:%d\n",key,sum);
			  context.write(key, result);//ÿ��IntWritable���Ƕ�Ӧ��key��Int�ļ��ϣ�����key-value�ķŽ�ȥ  
		  } 
	}
	
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(JTruthMain.config(),"step3");
        String input = path.get("Step5Input");
        String output = path.get("Step5Output");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(JStep5.class);
        job.setMapperClass(step5CountSourceMapper.class);
        job.setReducerClass(step5CountSourceReducer.class);
        
        for(int i= 1;i<=26;i++) {
        	if(i!=6  && i!=21 && i !=23) {
        	FileInputFormat.addInputPath(job, new Path(input+"/"+String.valueOf(i)));
        	}
        }
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);//��ִ����ϣ��˳�
	}
}
