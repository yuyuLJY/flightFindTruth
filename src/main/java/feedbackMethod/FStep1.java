package feedbackMethod;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FStep1 {
	static final int standardColumnsNumber = 8;
	public static class step1StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");
			context.write(new Text(splitResult[1]), value);//<"Nanhang",整条信息>
		}
	}
	public static class step1StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			  for(Text value : values) {//对每个航班的信息标准化
				  String[] saveSplitResult = value.toString().split("\t");
				  String[] standardEightColumn = new String [standardColumnsNumber]; 
					//TODO 初始全为-1
				  for(int ii=0;ii<standardColumnsNumber;ii++) {
						standardEightColumn[ii]="0";
				  }
				  int i = 0;
				  for(int j=0; j<standardColumnsNumber; j++) {//使用分割得到的行去填充标准的数组
						//i=0 || i=1
						if(i>=saveSplitResult.length) {
							break;
						}
						if(j<=1) {
							standardEightColumn[j] = saveSplitResult[i];
							i++;
						}
						//System.out.printf("j:%d i:%d %s\n",j,i,saveSplitResult[i]);
						//strSplitSplit = saveSplitResult[i].split(" ");//判断这个数据是不是日期
						//i =2
						if((j==2 || j==3)) {
							if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline")) {//有空格
								if(saveSplitResult[i].contains(":")) {//有冒号就是日期
									standardEightColumn[j] = saveSplitResult[i];
								}
								i++;
							}else {//长度为1
								j=4;//下一轮从5开始
								standardEightColumn[j] = saveSplitResult[i];
							}
						}
						//System.out.printf("%d %s\n",j,Arrays.toString(strSplitSplit));
						if(j==4 &&(!saveSplitResult[i].contains(":"))) {//""或者FD8
							if(saveSplitResult[i].equals("")==false && 
									(!saveSplitResult[i].contains("Not provided by airline"))) {//只有真实的值才会覆盖"0"
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}//分割结果是两位的：j下移，i不动
						if(j==5 || j==6) {
							//System.out.printf("当前列%s\n",Arrays.toString(strSplitSplit));
							if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline") ) {//有空格
								if(saveSplitResult[i].contains(":")) {
									standardEightColumn[j] = saveSplitResult[i];
								}
								i++;
							}else {//长度为1
								j=7;
								standardEightColumn[j] = saveSplitResult[i];
							}
						}
						if(j==7) {
							if(!saveSplitResult[i].equals("") && !saveSplitResult[i].equals("-") &&
									(!saveSplitResult[i].contains("Not provided by airline"))) {
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}
				  }
				  //把标准数组组成String
				  System.out.println("%s"+Arrays.toString(standardEightColumn));
				  String s =standardEightColumn[0];
				  for(int k = 1; k <standardColumnsNumber;k++) {
					  s =s+"/t"+standardEightColumn[k];
				  }
				  context.write(new Text(standardEightColumn[0]),new Text(s));
				}
			  //System.out.println("finish");
		  } 	
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(FeedbackMain.config(),"step1");
        String input = path.get("Step1Input");
        String output = path.get("Step1Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(FStep1.class);
        job.setMapperClass(step1StandardMapper.class);
        job.setReducerClass(step1StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
