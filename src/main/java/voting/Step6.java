package voting;

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

public class Step6 {
	static final int standardColumnsNumber = 7;
	public static class step6StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");
			context.write(new Text(splitResult[0]), value);//<"Nanhang",整条信息>
		}
	}
	public static class step6StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			  for(Text value : values) {//对每个航班的信息标准化
				  String[] saveSplitResult = value.toString().split("\t");
				  System.out.printf("输入的矩阵：%s\n",Arrays.toString(saveSplitResult));
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
						if(j<=2) {
							standardEightColumn[j] = saveSplitResult[i];
							i++;
						}
						if(j==3) {
							if(saveSplitResult[i].contains("--") || saveSplitResult[i].contains(":")) {
								standardEightColumn[j] = "0";
							}else {
								standardEightColumn[j] = saveSplitResult[i];//只有正确的才能移动
								i++;
							}
						}

						if(j==4 || j==5) {
							//System.out.printf("当前列%s\n",Arrays.toString(strSplitSplit));
							if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline") ) {//有空格
								if(saveSplitResult[i].contains(":")) {
									standardEightColumn[j] = saveSplitResult[i];
								}
								i++;
							}else {//长度为1
								standardEightColumn[j] = saveSplitResult[i];
							}
						}
						if(j==6) {
							if(saveSplitResult.length<standardColumnsNumber || saveSplitResult[i].contains("--") || saveSplitResult[i].contains(":")) {
								standardEightColumn[j] = "0";
							}else{
								standardEightColumn[j] = saveSplitResult[i];
							}
						}
				  }
				  //把标准数组组成String
				  System.out.printf("标准的矩阵：%s\n",Arrays.toString(standardEightColumn));
				  String s =standardEightColumn[0];
				  for(int k = 1; k <standardColumnsNumber;k++) {
					  s =s+"/t"+standardEightColumn[k];
				  }
				  System.out.printf("标准的句子：%s\n",s);
				  context.write(new Text(standardEightColumn[0]),new Text(s));
				}
			  System.out.println("finish");
		  } 	
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(TruthMain.config(),"step6");
        String input = path.get("Step6Input");
        String output = path.get("Step6Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step6.class);
        job.setMapperClass(step6StandardMapper.class);
        job.setReducerClass(step6StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}