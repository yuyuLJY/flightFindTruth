package voting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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



public class Step7 {
	//���պ������Ϣ����
	public static class step7Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap����
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"Nanhang",������Ϣ>
		}
	}
	
	public static class step7Reducer extends Reducer<Text,Text,IntWritable,IntWritable> {
		 IntWritable one = new IntWritable(1);  //�������ֵʼ����1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				 CorrectSituation tt = new CorrectSituation();
				 if(tt.getCorrecInfo().containsKey(key.toString())) {//�������
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//�ҵ��Ϻ�AT900����ȷ��Ϣ
				 //------------------��֤flightInfo-----------------------------
				 System.out.printf("��ȷ����� %s\n",Arrays.toString(flightInfo));
				 Map<String,Float> correctRate = tt.getCorrectRate();
				 for(Text value : values) {//
					 int predictFlag = 1 ;//Ԥ������
					 int actualFlag = 1;//��ʵ�����
					 
					//-------------------ʵ�ʵ����---------------
					 String[] saveSplitResult = value.toString().split("/t");//�ָ�
					 System.out.printf("��ϢԴ %s\n",Arrays.toString(saveSplitResult));
					 if(!saveSplitResult[4].equals("0") && !flightInfo[3].equals("0")&& !saveSplitResult[4].equals(flightInfo[3])) {//�жϵǻ���
						 actualFlag = 0;
					 }
					 if(!saveSplitResult[7].equals("0") && !flightInfo[6].equals("0")&&!saveSplitResult[7].equals(flightInfo[6])) {//�жϵ����
						 actualFlag = 0;
					 }
					 //�жϳ���ʱ��
					 for(int i =2;i<4;i++) {
						  if(!saveSplitResult[i].equals("0")) {//"0"�����
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if( timeTruth !=0 && timeTest!=timeTruth) {//�����ȷ��Ϣȱʧ�򲻱Ƚ�
								  actualFlag=0;
							  }
								  
						  }
					 }
					 //����ʱ���ж�
					 for(int i =5;i<7;i++) {
						  if(!saveSplitResult[i].equals("0")) {//"0"�����
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if(timeTruth !=0 && timeTest!=timeTruth) {
								  actualFlag=0;
							  }
								  
						  }
					 }
					 //-------------------Ԥ������---------------
					 if(correctRate.get(saveSplitResult[0])<0.5) {//�����ȷ��С��0.5������Ϊ���󣬼�0
						 predictFlag= 0;
					 }
					 System.out.printf("�ж���Ϣ %d %d\n",predictFlag,actualFlag);
					 context.write(new IntWritable(predictFlag),new IntWritable(actualFlag));
				 }
			}
		  }
	}
	
	//��������ʽ�����������ʱ��
	public static int CountTime(String s){
		  System.out.printf("�����ַ�����%s\n",s);
		  String actualTime = "0";
		  String[] split = s.split(" ");//�ѳ���ʱ���Ƭ
		  for(String t : split) {//�ҵ�����":"��ʱ��,���ǲ�Ҫ���������
			  if(t.contains(":") && !t.contains("(")) {
				  actualTime = t;
				  actualTime = actualTime.replaceAll(" ", "");//��ȥ���հ׷���
				  actualTime = actualTime.replaceAll("[a-zA-Z]", "");
				  //TODO something wrong
				  //+��*��|��\�ȷ�����������ʾ������Ӧ�Ĳ�ͬ����
				  actualTime = actualTime.replaceAll("\\*", "");
			  }
		  }
		  System.out.printf("�ҵ��ĵ���ʱ��  %s\n",actualTime);
		  int CountTime =0;
		  if(actualTime.contains(":")) {
			  String[] splitCountTime = actualTime.split(":");
			  CountTime = Integer.parseInt(splitCountTime[0])*60+Integer.parseInt(splitCountTime[1]);  
		  }
		  return CountTime;
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		//�ȶ�����ȷ���ı�
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		
		//TODO ������ϢԴ��������ִ��ʮ�飬��������������
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/standard_truth_flight/part-r-00000");
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Exists!");
			try {
				//��Ϊhadoop��ȡ��������
				FSDataInputStream is = fs.open(pathSourceNumber);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //�����ݶ��뵽��������
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //�ӻ������ж�ȡ����
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	String[] splitValue = split[1].split("/t"); 
                	tt.setCorrectInfo(split[0],splitValue);//д��ϢԴ������
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
		//---------------------��ȡ��ȷ��---------------------------
		Path pathCorrectRate = new Path("hdfs://192.168.126.130:9000/user/findTruth/step5/correctRate.txt");
		if (fs.exists(pathCorrectRate)) {
			System.out.println("Exists!");
			try {
				//��Ϊhadoop��ȡ��������
				FSDataInputStream is = fs.open(pathCorrectRate);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //�����ݶ��뵽��������
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //�ӻ������ж�ȡ����
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	tt.setCorrectRate(split[0],Float.parseFloat(split[1]));//д��ȷ��
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
		//----------------------------��֤������ȷ
		Map<String,String[]> correcInfo = tt.getCorrecInfo();
		System.out.println("��֤��ȷ��Ϣ");
		for(String s : correcInfo.keySet()) {
			System.out.printf("%s %s\n",s,Arrays.toString(correcInfo.get(s)));
		}
		
		Map<String,Float> correctrate  = tt.getCorrectRate();
		System.out.println("��֤��ȷ��");
		for(String s : correctrate.keySet()) {
			System.out.printf("%s %f\n",s,correctrate.get(s));
		}
		
		
		Job job = new Job(TruthMain.config(),"step7");
        String input = path.get("Step7Input");
        String output = path.get("Step7Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(Step7.class);
        job.setMapperClass(step7Mapper.class);
        job.setReducerClass(step7Reducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//��ִ����ϣ��˳�
	}
}
