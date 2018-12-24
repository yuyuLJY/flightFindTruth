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
 * ���룺ÿ���µı�׼�ĺ�����Ϣ
 * ����������ȷ����ϢԴд�ɣ� <"travel",1>��д��"step/i"��
 * �Ȱ���ȷ�ı�׼Truth����correctInfo���,Ȼ���ñ�׼�𰸸�ѵ�����Ա�
 * */
public class JStep4 {
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
	public static class step4Reducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //�������ֵʼ����1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				 CorrectSituation tt = new CorrectSituation();
				 if(tt.getCorrecInfo().containsKey(key.toString())) {//�������
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//�ҵ��Ϻ�AT900����ȷ��Ϣ
				 //------------------��֤flightInfo-----------------------------
				 System.out.printf("��ȷ����� %s\n",Arrays.toString(flightInfo));
				 for(Text value : values) {
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
					 System.out.println("�жϽ����"+actualFlag);
					 //---------------�������ѵ������ȷ���������Դ�ӽ�ȥ---------------
					 if(actualFlag==1) {
						 context.write(new Text(saveSplitResult[0]),one); 
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
	public static void run(String inputName,String outputName,Map<String, String> pathTruth,Map<String, String> step4Path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//�ȶ�����ȷ���ı�
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=JTruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO ������ϢԴ�ı�׼�����ִ��ʮ�飬��������������
		//Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/standard_truth_flight/part-r-00000");
		System.out.println("��ȡÿ�±�׼�𰸣�"+pathTruth.get(outputName)+"/part-r-00000");
		Path pathSourceNumber = new Path(pathTruth.get(outputName)+"/part-r-00000");
		tt.getCorrecInfo().clear();//!!!!!ͳ��ÿ���µ�ʱ��Ҫ���
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
                	tt.setCorrectInfo(split[0],splitValue);//д��ϢԴ�����
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
		//Map<String,String[]> correcInfo = tt.getCorrecInfo();
		//System.out.println("��֤��ȷ��Ϣ");
		//for(String s : correcInfo.keySet()) {
			//System.out.printf("%s %s\n",s,Arrays.toString(correcInfo.get(s)));
		//}
		
		Job job = new Job(JTruthMain.config(),"step4");
		System.out.println("����MR�����ݼ���"+step4Path.get(inputName));
		System.out.println("д��MR�����ݼ���"+step4Path.get(outputName));
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
		job.waitForCompletion(true);//��ִ����ϣ��˳�
		
	}
}
