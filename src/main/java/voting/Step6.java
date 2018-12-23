package voting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Step6 {
	public static class step6Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap����
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitResult[1]), value);//<"Nanhang",������Ϣ>
		}
	}
	public static class step2CountScoreReducer extends Reducer<Text,Text,IntWritable,IntWritable> {
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 CorrectSituation tt = new CorrectSituation();
			 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//�ҵ��Ϻ�AT900����ȷ��Ϣ
			 Map<String,Float> correctRate = tt.getCorrectRate();
			 for(Text value : values) {//
				 int predictFlag = 1 ;//Ԥ������
				 int actualFlag = 1;//��ʵ�����
				 
				//-------------------ʵ�ʵ����---------------
				 String[] saveSplitResult = value.toString().split("/t");//�ָ�
				 if(!saveSplitResult[4].equals(flightInfo[4])) {//�жϵǻ���
					 actualFlag = 0;
				 }
				 if(!saveSplitResult[7].equals(flightInfo[7])) {//�жϵ����
					 actualFlag = 0;
				 }
				 //�жϳ���ʱ��
				 for(int i =2;i<4;i++) {
					  if(!saveSplitResult[i].equals("0")) {//"0"�����
						  int timeTest = CountTime(saveSplitResult[i]);
						  int timeTruth = CountTime(flightInfo[i]);
						  if(timeTest!=timeTruth) {
							  actualFlag=0;
						  }
							  
					  }
				 }
				 //����ʱ���ж�
				 for(int i =5;i<7;i++) {
					  if(!saveSplitResult[i].equals("0")) {//"0"�����
						  int timeTest = CountTime(saveSplitResult[i]);
						  int timeTruth = CountTime(flightInfo[i]);
						  if(timeTest!=timeTruth) {
							  actualFlag=0;
						  }
							  
					  }
				 }
				 //-------------------Ԥ������---------------
				 if(correctRate.get(saveSplitResult[0])<0.5) {//�����ȷ��С��0.5������Ϊ���󣬼�0
					 predictFlag= 0;
				 }
				 
				 context.write(new IntWritable(predictFlag),new IntWritable(actualFlag));
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
		  System.out.printf("�ҵ��ĵ���ʱ��2  %s\n",actualTime);
		  String[] splitCountTime = actualTime.split(":");
		  int CountTime = Integer.parseInt(splitCountTime[0])*60+Integer.parseInt(splitCountTime[1]);
		  return CountTime;
	}
	
	public static void run() throws IOException {
		//�ȶ�����ȷ���ı�
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO ������ϢԴ��������ִ��ʮ�飬��������������
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/flight_truth/2011-12-27-truth.txt");
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
                	tt.setCorrectInfo(split[0],split);//д��ϢԴ������
                    System.out.println("line="+line);
                    
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
                	tt.setCorrectRate(split[0],Integer.parseInt(split[1]));//д��ϢԴ������
                    System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
	}
}
