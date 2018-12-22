package com.yuyu;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
public class Step1 {
	static final int standardColumnsNumber = 8;
	public static class step1StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");
			context.write(new Text(splitResult[1]), value);//<"Nanhang",������Ϣ>
		}
	}
	public static class step1StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			  for(Text value : values) {//��ÿ���������Ϣ��׼��
				  String[] saveSplitResult = value.toString().split("\t");
				  String[] standardEightColumn = new String [standardColumnsNumber]; 
					//TODO ��ʼȫΪ-1
				  for(int ii=0;ii<standardColumnsNumber;ii++) {
						standardEightColumn[ii]="0";
				  }
				  int i = 0;
				  for(int j=0; j<standardColumnsNumber; j++) {//ʹ�÷ָ�õ�����ȥ����׼������
						//i=0 || i=1
						if(i>=saveSplitResult.length) {
							break;
						}
						if(j<=1) {
							standardEightColumn[j] = saveSplitResult[i];
							i++;
						}
						//System.out.printf("j:%d i:%d %s\n",j,i,saveSplitResult[i]);
						//strSplitSplit = saveSplitResult[i].split(" ");//�ж���������ǲ�������
						//i =2
						if((j==2 || j==3)) {
							if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline")) {//�пո�
								if(saveSplitResult[i].contains(":")) {//��ð�ž�������
									standardEightColumn[j] = saveSplitResult[i];
								}
								i++;
							}else {//����Ϊ1
								j=4;//��һ�ִ�5��ʼ
								standardEightColumn[j] = saveSplitResult[i];
							}
						}
						//System.out.printf("%d %s\n",j,Arrays.toString(strSplitSplit));
						if(j==4 &&(!saveSplitResult[i].contains(":"))) {//""����FD8
							if(saveSplitResult[i].equals("")==false && 
									(!saveSplitResult[i].contains("Not provided by airline"))) {//ֻ����ʵ��ֵ�ŻḲ��"0"
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}//�ָ�������λ�ģ�j���ƣ�i����
						if(j==5 || j==6) {
							//System.out.printf("��ǰ��%s\n",Arrays.toString(strSplitSplit));
							if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline") ) {//�пո�
								if(saveSplitResult[i].contains(":")) {
									standardEightColumn[j] = saveSplitResult[i];
								}
								i++;
							}else {//����Ϊ1
								j=7;
								standardEightColumn[j] = saveSplitResult[i];
							}
						}
						if(j==7) {
							if(!saveSplitResult[i].equals("") && 
									(!saveSplitResult[i].contains("Not provided by airline"))) {
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}
				  }
				  //�ѱ�׼�������String
				  String s =standardEightColumn[0];
				  for(int k = 1; k <standardColumnsNumber;k++) {
					  s =s+"/t"+standardEightColumn[k];
				  }
				  context.write(new Text(standardEightColumn[0]),new Text(s));
				}
		  } 	
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(TruthMain.config(),"step1");
        String input = path.get("Step1Input");
        String output = path.get("Step1Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step1.class);
        job.setMapperClass(step1StandardMapper.class);
        job.setReducerClass(step1StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//��ִ����ϣ��˳�
	}
}
