package com.yuyu;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
public class Step1 {
	public static class step1StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");
			context.write(new Text(splitResult[1]), value);//<"Nanhang",������Ϣ>
			System.out.println("map");
		}
	}
	public static class step1StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			  for(Text value : values) {//��ÿ���������Ϣ��׼��
				  
			  }
		  } 	
	}
	
	public static void run(Map<String, String> path) throws IOException{
		
	}
}
