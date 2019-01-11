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

//ͳ����ȷ��
public class FStep4 {
	//���պ������Ϣ����
	public static class step4Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap����
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"Nanhang",������Ϣ>
		}
	}
	
	//����ȷ����ϢԴд�ɣ� <"travel",1>
	public static class step4Reducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				 FCorrectSituation tt = new FCorrectSituation();
				 if(tt.getCorrecInfo().containsKey(key.toString())) {//�������
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//�ҵ��Ϻ�AT900����ȷ��Ϣ
				 //------------------��֤flightInfo-----------------------------
				 System.out.printf("Step4��ȷ����� %s\n",Arrays.toString(flightInfo));
				 for(Text value : values) {
					//-------------------ʵ�ʵ����---------------
					 String[] saveSplitResult = value.toString().split("/t");//�ָ�
					 System.out.printf("��ϢԴ %s\n",Arrays.toString(saveSplitResult));
					 
					 //------------------�жϵǻ���---------------
					 int cateActualFlag1 = 1;//��ʵ�����
					 int catePredictFlag1=1;
					 int cateActualFlag2 = 1;//��ʵ�����
					 int catePredictFlag2=1;
					 if(!saveSplitResult[4].equals("0") && !flightInfo[3].equals("0")) {
						 if(!saveSplitResult[4].equals(flightInfo[3])) {//�жϵǻ���
							 cateActualFlag1 = 0;
						 }
						 if(tt.getRealityI().get(saveSplitResult[0])<0.5) {
							 catePredictFlag1=0;
						 }
						 String s = String.valueOf(catePredictFlag1)+"/t"+String.valueOf(cateActualFlag1);
						 context.write(new Text(saveSplitResult[0]),new Text(s)); 
					 }
					 
					 if(!saveSplitResult[7].equals("0") && !flightInfo[6].equals("0")) {
						 if(!saveSplitResult[7].equals(flightInfo[6])) {//�жϵǻ���
							 cateActualFlag2 = 0;
						 }
						 if(tt.getRealityI().get(saveSplitResult[0])<0.5) {
							 catePredictFlag2=0;
						 }
						 String s = String.valueOf(catePredictFlag2)+"/t"+String.valueOf(cateActualFlag2);
						 context.write(new Text(saveSplitResult[0]),new Text(s)); 
					 }

					 //------------------�жϳ���ʱ��-----------------
					 for(int i =2;i<4;i++) {
						 int conActualFlag1 = 1;//��������ʵ���
						 int conPredictFlag1=1;
						 if(!saveSplitResult[i].equals("0")) {//"0"�����
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if( timeTruth !=0 && timeTest!=timeTruth) {//�����ȷ��Ϣȱʧ�򲻱Ƚ�
								  conActualFlag1=0;
							  }
							  if(tt.getRealityJ().get(saveSplitResult[0])<0.5) {
								  conPredictFlag1=0;  
							  }
							  String s = String.valueOf(catePredictFlag2)+"/t"+String.valueOf(cateActualFlag2);
							  context.write(new Text(saveSplitResult[0]),new Text(s));   
						 }
					 }
					 //����ʱ���ж�
					 for(int i =5;i<7;i++) {
						 int conActualFlag2 = 1;//��ʵ�����
						 int conPredictFlag2=1;
						  if(!saveSplitResult[i].equals("0")) {//"0"�����
							  int timeTest = CountTime(saveSplitResult[i]);
							  int timeTruth = CountTime(flightInfo[i-1]);
							  if( timeTruth !=0 && timeTest!=timeTruth) {//�����ȷ��Ϣȱʧ�򲻱Ƚ�
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
	
	//��������ʽ�����������ʱ��
	public static int CountTime(String s){
		  //System.out.printf("�����ַ�����%s\n",s);
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
		  //System.out.printf("�ҵ��ĵ���ʱ��  %s\n",actualTime);
		  int CountTime =0;
		  if(actualTime.contains(":")) {
			  String[] splitCountTime = actualTime.split(":");
			  CountTime = Integer.parseInt(splitCountTime[0])*60+Integer.parseInt(splitCountTime[1]);  
		  }
		  return CountTime;
	}
	
	//�������ݼ����ļ������֣�������ݼ��ļ��е����֣���׼�𰸵�λ�ã����ζ�д�ļ��е�λ��
	public static void run(Map<String, String> path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//�ȶ�����ȷ���ı�
		FCorrectSituation tt = new FCorrectSituation();
		
		Configuration conf=FeedbackMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.131:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO ������ϢԴ�ı�׼�����ִ��ʮ�飬��������������
		//Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/standard_truth_flight/part-r-00000");
		//System.out.println("��ȡÿ�±�׼�𰸣�"+pathTruth.get(outputName)+"/part-r-00000");
		//Path pathSourceNumber = new Path(pathTruth.get(outputName)+"/part-r-00000");
		Path pathSourceNumber = new Path("hdfs://192.168.126.131:9000/user/findTruth/feedback/data/TestTruth1211/part-r-00000");
		tt.getCorrecInfo().clear();//!!!!!ͳ��ÿ���µ�ʱ��Ҫ���
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Step4:Exists!");
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
                	tt.setCorrectInfo(split[0],splitValue);//д��ϢԴ�����
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("Step4������");
		}
		
		//----------------------------��֤������ȷ
		//Map<String,String[]> correcInfo = tt.getCorrecInfo();
		//System.out.println("��֤��ȷ��Ϣ");
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
		job.waitForCompletion(true);//��ִ����ϣ��˳�
		
	}
}
