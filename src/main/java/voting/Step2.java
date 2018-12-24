package voting;

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
	//���պ������Ϣ����
	public static class step2CountScoreMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap����
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"Nanhang",������Ϣ>
		}
	}
	
	public static class step2CountScoreReducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //�������ֵʼ����1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  Map<String,Integer> departureGate = new HashMap<String,Integer>();
			  Map<String,Integer> arriveGate = new HashMap<String,Integer>();
			  Map<Integer,Integer> departureTime1 = new HashMap<Integer,Integer>();
			  Map<Integer,Integer> departureTime2 = new HashMap<Integer,Integer>();
			  Map<Integer,Integer> arriveTime1 = new HashMap<Integer,Integer>();
			  Map<Integer,Integer> arriveTime2 = new HashMap<Integer,Integer>();
			  //�������
			  //��������ε���Ϣ
			  ArrayList<String[]> flightInfo = new ArrayList<String[]>();
			  /*--------------------����------------------------*/
			  for(Text value : values) {//e.g �Ϻ�AMT-100
				  //�����ǻ���
				  //System.out.printf("%s\n",value);
				  String[] saveSplitResult = value.toString().split("/t");
				  //System.out.printf("%s\n",Arrays.toString(saveSplitResult));
				  String departureGateName = saveSplitResult[4].replaceAll(" ", "");//��׼��������
				  if(departureGate.containsKey(departureGateName)==false) {//��û�а��������
					  departureGate.put(departureGateName, 1);
				  }else {//������Ϣ�Ѿ�������
					  departureGate.put(departureGateName,(departureGate.get(departureGateName)+1));
				  }
				  //����ǻ���
				  String arriveGateName = saveSplitResult[7].replaceAll(" ", "");//��׼��������
				  if(arriveGate.containsKey(arriveGateName)==false) {//��û�а��������
					  arriveGate.put(arriveGateName, 1);
				  }else {//������Ϣ�Ѿ�������
					  arriveGate.put(arriveGateName,arriveGate.get(arriveGateName)+1);
				  }
				  //������Ԥ��ʱ��t1
				  //System.out.printf("%s \n",saveSplitResult[2]);
				  String actualTime1 = "0";
				  if(!saveSplitResult[2].equals("0")) {//"0"�������
					  String[] splitDepartureTime1 = saveSplitResult[2].split(" ");//�ѳ���ʱ���Ƭ
					  for(String t1 : splitDepartureTime1) {//�ҵ�����":"��ʱ��,���ǲ�Ҫ���������
						  if(t1.contains(":") && !t1.contains("(")) {
							  actualTime1 = t1;
							  actualTime1 = actualTime1.replaceAll(" ", "");//��ȥ���հ׷���
							  actualTime1 = actualTime1.replaceAll("[a-zA-Z]", "");
							  actualTime1 = actualTime1.replaceAll("\\*", "");
						  }
					  }
					  System.out.printf("�ҵ��ĳ���ʱ��1  %s\n",actualTime1);
					  String[] splitCountTime1 = actualTime1.split(":");
					  int CountTime1 = Integer.parseInt(splitCountTime1[0])*60+Integer.parseInt(splitCountTime1[1]);
					  if(!departureTime1.containsKey(CountTime1)) {//����������ʱ��
						  departureTime1.put(CountTime1, 1);
					  }else {
						  departureTime1.put(CountTime1,departureTime1.get(CountTime1)+1);
					  }
					  saveSplitResult[2] = String.valueOf(CountTime1);//"460"
				  }//�������"0"

				//������ʵ��ʱ��t2
				  String actualTime2 = "0";
				  if(!saveSplitResult[3].equals("0")) {//"0"�������
					  String[] splitDepartureTime2 = saveSplitResult[3].split(" ");//�ѳ���ʱ���Ƭ
					  for(String t2 : splitDepartureTime2) {//�ҵ�����":"��ʱ��,���ǲ�Ҫ���������
						  if(t2.contains(":") && !t2.contains("(")) {
							  actualTime2 = t2;
							  actualTime2 = actualTime2.replaceAll(" ", "");//��ȥ���հ׷���
							  actualTime2 = actualTime2.replaceAll("[a-zA-Z]", "");
							  actualTime2 = actualTime2.replaceAll("\\*", "");
						  }
					  }
					  System.out.printf("�ҵ��ĳ���ʱ��2  %s\n",actualTime2);
					  String[] splitCountTime2 = actualTime2.split(":");
					  int CountTime2 = Integer.parseInt(splitCountTime2[0])*60+Integer.parseInt(splitCountTime2[1]);
					  if(!departureTime2.containsKey(CountTime2)) {//����������ʱ��
						  departureTime2.put(CountTime2, 1);
					  }else {
						  departureTime2.put(CountTime2,departureTime2.get(CountTime2)+1);
					  }
					  saveSplitResult[3] = String.valueOf(CountTime2);//"460"
				  }//�������"0"
				  
				//�����ʵ��ʱ��t1
				  String actualTime3 = "0";
				  if(!saveSplitResult[5].equals("0")) {//"0"�������
					  String[] splitArriveTime1 = saveSplitResult[5].split(" ");//�ѳ���ʱ���Ƭ
					  for(String t1 : splitArriveTime1) {//�ҵ�����":"��ʱ��,���ǲ�Ҫ���������
						  if(t1.contains(":") && !t1.contains("(")) {
							  actualTime3 = t1;
							  actualTime3 = actualTime3.replaceAll(" ", "");//��ȥ���հ׷���
							  actualTime3 = actualTime3.replaceAll("[a-zA-Z]", "");
							  actualTime3 = actualTime3.replaceAll("\\*", "");
						  }
					  }
					  System.out.printf("�ҵ��ĵ���ʱ��1  %s\n",actualTime3);
					  String[] splitCountTime3 = actualTime3.split(":");
					  int CountTime3 = Integer.parseInt(splitCountTime3[0])*60+Integer.parseInt(splitCountTime3[1]);
					  if(!arriveTime1.containsKey(CountTime3)) {//����������ʱ��
						  arriveTime1.put(CountTime3, 1);
					  }else {
						  arriveTime1.put(CountTime3,arriveTime1.get(CountTime3)+1);
					  }
					  saveSplitResult[5] = String.valueOf(CountTime3);//"460"
				  }//�������"0"
				  
				//�����ʵ��ʱ��t2
				  String actualTime4 = "0";
				  if(!saveSplitResult[6].equals("0")) {//"0"�������
					  System.out.printf("saveSplitResult[6]��%s\n",saveSplitResult[6]);
					  String[] splitArriveTime2 = saveSplitResult[6].split(" ");//�ѳ���ʱ���Ƭ
					  for(String t2 : splitArriveTime2) {//�ҵ�����":"��ʱ��,���ǲ�Ҫ���������
						  if(t2.contains(":") && !t2.contains("(")) {
							  actualTime4 = t2;
							  actualTime4 = actualTime4.replaceAll(" ", "");//��ȥ���հ׷���
							  actualTime4 = actualTime4.replaceAll("[a-zA-Z]", "");
							  //TODO something wrong
							  //+��*��|��\�ȷ�����������ʾ������Ӧ�Ĳ�ͬ����
							  actualTime4 = actualTime4.replaceAll("\\*", "");
						  }
					  }
					  System.out.printf("�ҵ��ĵ���ʱ��2  %s\n",actualTime4);
					  String[] splitCountTime4 = actualTime4.split(":");
					  int CountTime4 = Integer.parseInt(splitCountTime4[0])*60+Integer.parseInt(splitCountTime4[1]);
					  if(!arriveTime2.containsKey(CountTime4)) {//����������ʱ��
						  arriveTime2.put(CountTime4, 1);
					  }else {
						  arriveTime2.put(CountTime4,arriveTime2.get(CountTime4)+1);
					  }
					  saveSplitResult[6] = String.valueOf(CountTime4);//"460"
				  }//�������"0"
				  //System.out.println("��ѭ��");
				flightInfo.add(saveSplitResult);//��������  
			  }
			  /*
			  System.out.println("�鿴�����������");
			  for(String s : departureGate.keySet()) {
				  System.out.printf("%s %d\n",s,departureGate.get(s));  
			  }*/
			  System.out.println("�鿴����t2���");
			  for(int s : arriveTime2.keySet()) {
				  System.out.printf("%s %d\n",s,arriveTime2.get(s));  
			  }
			  
			  /*--------------------ѡ��key���ֵ------------------------*/
			  //�����ں͵����
			  int maxDepartureGate = 0;
			  String maxDepartureGateKey = "";
			  int maxArriveGate = 0;
			  String maxArriveGateKey = "0";
			  for(String s : departureGate.keySet()) {
				  if(departureGate.get(s)>maxDepartureGate && !s.equals("0")) {
					  maxDepartureGate = departureGate.get(s);
					  maxDepartureGateKey = s;
				  }
			  }
			  for(String s : arriveGate.keySet()) {
				  if(arriveGate.get(s)>maxArriveGate && !s.equals("0")) {//���Ҳ�����0
					  maxArriveGate = arriveGate.get(s);
					  maxArriveGateKey = s;
				  }
			  }
			  //����ʱ�����Ϣt1
			  int maxDepartureTime1Vuale = 0;
			  int maxDepartureTime1Key = 0;
			  for(int t1 : departureTime1.keySet()) {
				  if(departureTime1.get(t1)>maxDepartureTime1Vuale ) {
					  maxDepartureTime1Vuale = departureTime1.get(t1);
					  maxDepartureTime1Key = t1;
				  }
			  }
			  //System.out.printf("%d \n",maxDepartureTime1Key);
			//����ʱ�����Ϣt2
			  int maxDepartureTime2Vuale = 0;
			  int maxDepartureTime2Key = 0;
			  for(int t2 : departureTime2.keySet()) {
				  if(departureTime2.get(t2)>maxDepartureTime2Vuale ) {
					  maxDepartureTime2Vuale = departureTime2.get(t2);
					  maxDepartureTime2Key = t2;
				  }
			  }
			  //System.out.printf("%d \n",maxDepartureTime2Key);
			//����ʱ�����Ϣt1
			  int maxArriveTime1Vuale = 0;
			  int maxArriveTime1Key = 0;
			  for(int t1 : arriveTime1.keySet()) {
				  if(arriveTime1.get(t1)>maxArriveTime1Vuale ) {
					  maxArriveTime1Vuale = arriveTime1.get(t1);
					  maxArriveTime1Key = t1;
				  }
			  }
			  //System.out.printf("%d \n",maxArriveTime1Key);
			//�ﵽʱ�����Ϣt2
			  int maxArriveTime2Vuale = 0;
			  int maxArriveTime2Key = 0;
			  for(int t2 : arriveTime2.keySet()) {
				  if(arriveTime2.get(t2)>maxArriveTime2Vuale ) {
					  maxArriveTime2Vuale = arriveTime2.get(t2);
					  maxArriveTime2Key = t2;
				  }
			  }
			  System.out.printf("Key: %d \n",maxArriveTime2Key);
			  
			  
			  /*--------------����Ӧ����ϢԴ��ֵ----------------*/
			  for(String[] saveSplitResult : flightInfo) {
				  //String[] saveSplitResult = .toString().split("/t");
				  System.out.printf("%s\n",Arrays.toString(saveSplitResult));
				  //System.out.printf("�������㣺 %s ��Ϣ��%s\n",maxDepartureGateKey,(saveSplitResult[4].replaceAll(" ", "")));
				  //�����ں͵����
				  if((saveSplitResult[4].replaceAll(" ", "")).equals(maxDepartureGateKey)) {
					  //System.out.printf("��ѡ�е���ϢԴ��%s\n",saveSplitResult[0]);
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  if((saveSplitResult[7].replaceAll(" ", "")).equals(maxArriveGateKey)) {
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  //����ʱ��t1
				  //���¼��������ʱ��
				  if(saveSplitResult[2].equals(String.valueOf(maxDepartureTime1Key))) {
					  //System.out.printf("���\n");
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  //����ʱ��t2
				  if(saveSplitResult[3].equals(String.valueOf(maxDepartureTime2Key))) {
					  //System.out.printf("���\n");
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  //����ʱ��t1
				  if(saveSplitResult[5].equals(String.valueOf(maxArriveTime1Key))) {
					  //System.out.printf("���\n");
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  //�ﵽʱ��t2
				  if(saveSplitResult[6].equals(String.valueOf(maxArriveTime2Key))) {
					  System.out.printf("���\n");
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
		System.exit(job.waitForCompletion(true) ? 0 : 1);//��ִ����ϣ��˳�
	}
}
