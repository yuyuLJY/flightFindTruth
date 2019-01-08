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


public class FStep2 {
	static final int standardColumnsNumber = 7;
	public static class step2StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");
			context.write(new Text(splitResult[0]), value);//<"Nanhang",������Ϣ>
		}
	}
	public static class step2StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			  for(Text value : values) {//��ÿ���������Ϣ��׼��
				  String[] saveSplitResult = value.toString().split("\t");
				  System.out.printf("����ľ���%s\n",Arrays.toString(saveSplitResult));
				  String[] standardSevenColumn = new String [standardColumnsNumber]; 
					//TODO ��ʼȫΪ-1
				  for(int ii=0;ii<standardColumnsNumber;ii++) {
					  standardSevenColumn[ii]="0";
				  }
				  int i = 0;
				  for(int j=0; j<standardColumnsNumber; j++) {//ʹ�÷ָ�õ�����ȥ����׼������
						//i=0 || i=1
						if(i>=saveSplitResult.length) {
							break;
						}
						if(j<=2) {
							standardSevenColumn[j] = saveSplitResult[i];
							i++;
						}
						if(j==3) {
							if(saveSplitResult[i].contains("--") || saveSplitResult[i].contains(":")) {
								standardSevenColumn[j] = "0";
							}else {
								standardSevenColumn[j] = saveSplitResult[i];//ֻ����ȷ�Ĳ����ƶ�
								i++;
							}
						}

						if(j==4 || j==5) {
							//System.out.printf("��ǰ��%s\n",Arrays.toString(strSplitSplit));
							if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline") ) {//�пո�
								if(saveSplitResult[i].contains(":")) {
									standardSevenColumn[j] = saveSplitResult[i];
								}
								i++;
							}else {//����Ϊ1
								standardSevenColumn[j] = saveSplitResult[i];
							}
						}
						if(j==6) {
							if(saveSplitResult.length<standardColumnsNumber || saveSplitResult[i].contains("--") || saveSplitResult[i].contains(":")) {
								standardSevenColumn[j] = "0";
							}else{
								standardSevenColumn[j] = saveSplitResult[i];
							}
						}
				  }
				  //�ѱ�׼�������String
				  System.out.printf("��׼�ľ���%s\n",Arrays.toString(standardSevenColumn));
				  String s =standardSevenColumn[0];
				  for(int k = 1; k <standardColumnsNumber;k++) {
					  s =s+"/t"+standardSevenColumn[k];
				  }
				  System.out.printf("��׼�ľ��ӣ�%s\n",s);
				  context.write(new Text(standardSevenColumn[0]),new Text(s));
				}
			  System.out.println("finish");
		  } 	
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(FeedbackMain.config(),"step2");
        String input = path.get("Step2Input");
        String output = path.get("Step2Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(FStep2.class);
        job.setMapperClass(step2StandardMapper.class);
        job.setReducerClass(step2StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);
	}
}
