package com.yuyu;

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
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"Nanhang",������Ϣ>
		}
	}
	
	public static class step2CountScoreReducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //�������ֵʼ����1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  Map<String,Integer> departureGate = new HashMap<String,Integer>();
			  Map<String,Integer> arriveGate = new HashMap<String,Integer>();
			  //�������
			  //��������ε���Ϣ
			  ArrayList<String[]> flightInfo = new ArrayList<String[]>();
			  for(Text value : values) {//e.g �Ϻ�AMT-100
				  //�ȼ���
				  //�����ǻ���
				  //System.out.printf("%s\n",value);
				  String[] saveSplitResult = value.toString().split("/t");
				  flightInfo.add(saveSplitResult);
				  System.out.printf("%s\n",Arrays.toString(saveSplitResult));
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
			  }
			  System.out.println("�鿴�����������");
			  for(String s : departureGate.keySet()) {
				  System.out.printf("%s %d\n",s,departureGate.get(s));  
			  }
			  //ѡ��key���ֵ
			  int maxDepartureGate = 0;
			  String maxDepartureGateKey = "";
			  int maxArriveGate = 0;
			  String maxArriveGateKey = "";
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
			  //����Ӧ����ϢԴ��ֵ
			  for(String[] saveSplitResult : flightInfo) {
				  //String[] saveSplitResult = .toString().split("/t");
				  //System.out.printf("%s\n",Arrays.toString(saveSplitResult));
				  //System.out.printf("�������㣺 %s ��Ϣ��%s\n",maxDepartureGateKey,(saveSplitResult[4].replaceAll(" ", "")));
				  if((saveSplitResult[4].replaceAll(" ", "")).equals(maxDepartureGateKey)) {
					  System.out.printf("��ѡ�е���ϢԴ��%s\n",saveSplitResult[0]);
					  context.write(new Text(saveSplitResult[0]),one);
				  }
				  if((saveSplitResult[7].replaceAll(" ", "")).equals(maxArriveGateKey)) {
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
