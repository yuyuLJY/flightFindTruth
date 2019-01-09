package feedbackMethod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

public class FStep3 {
	static double alpha = 0.000004985;
	//���պ������Ϣ����	
	public static class step3Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap����
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"MP190",������Ϣ>
		}
	}
	
	//����ȷ����ϢԴд�ɣ� <"travel",1>
	public static class step3Reducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //�������ֵʼ����1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			FCorrectSituation tt = new FCorrectSituation();
			if(tt.getCorrecInfo().containsKey(key.toString())) {//�������
				 System.out.println("��֤Dcon�Ĵ�С��"+tt.getDcon().size());
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//�ҵ��Ϻ�AT900����ȷ��Ϣ
				 ArrayList<String []> collectInfo = new ArrayList<String[]>(); //��38����ϢԴ�����ĺ�����Ϣ�ռ�����
				 //------------------��֤flightInfo-----------------------------
				 System.out.printf("��ȷ����� %s\n",Arrays.toString(flightInfo));
				 for(Text value : values) {
					 double IsDepartureGateCorrect = 1;
					 double IsArriveGateCorrect = 1;
					//-------------------ʵ�ʵ����---------------
					 String[] saveSplitResult = value.toString().split("/t");//�ָ�
					 collectInfo.add(saveSplitResult);//��ÿ����Ϣ��������
					 //System.out.printf("��ϢԴ %s\n",Arrays.toString(saveSplitResult));
					 //�жϵǻ��ں͵����
					 if(!saveSplitResult[4].equals("0") && !flightInfo[3].equals("0")&& !saveSplitResult[4].equals(flightInfo[3])) {//�жϵǻ���
						 IsDepartureGateCorrect = 0;
					 }
					 if(!saveSplitResult[7].equals("0") && !flightInfo[6].equals("0")&&!saveSplitResult[7].equals(flightInfo[6])) {//�жϵ����
						 IsArriveGateCorrect = 0;
					 }
					 tt.setDcate(saveSplitResult[0], IsArriveGateCorrect+IsDepartureGateCorrect);
				 }
				//--------------------�ռ���Ϣ������-----------------
				double sumTime1=0,sumTime2 = 0,sumTime3=0,sumTime4 = 0;//������ϢԴ����ʱ���
				double AveTime1=0,AveTime2 = 0,AveTime3=0,AveTime4 = 0;//������ϢԴ������ϢԴ����ʱ��ƽ��ֵ
				double Distance1=0,Distance2 = 0,Distance3=0,Distance4 = 0;//����Ϣ��ƽ����
				double absDistance1=0,absDistance2 = 0,absDistance3=0,absDistance4 = 0;//����ֵ����
				double countLoss1=0,countLoss2 = 0,countLoss3=0,countLoss4 = 0;//
				double number = collectInfo.size();//һ���ж��ٸ���ϢԴ�������
				//����ÿ�е��ܺ�
				for(String[] s:collectInfo) {
					sumTime1 += CountTime(s[2]);
					sumTime2 += CountTime(s[3]);
					sumTime3 += CountTime(s[5]);
					sumTime4 += CountTime(s[6]);
				}
				//����ƽ��ֵ
				AveTime1 = sumTime1/number;
				AveTime2 = sumTime2/number;
				AveTime3 = sumTime3/number;
				AveTime4 = sumTime4/number;
				//����con��ĸ
				for(String[] s:collectInfo) {
					Distance1 +=  Math.pow(CountTime(s[2])-AveTime1,2);
					Distance2 +=  Math.pow(CountTime(s[3])-AveTime2,2);
					Distance3 +=  Math.pow(CountTime(s[5])-AveTime3,2);
					Distance4 +=  Math.pow(CountTime(s[6])-AveTime4,2);
					System.out.println("Distance1:"+Distance1+"Distance2:"+Distance2+"Distance3:"+Distance3+"Distance4:"+Distance4);
				}
				//�����ĸ�������Ժ��ֵ
				absDistance1 = Math.sqrt(Distance1);
				absDistance2 = Math.sqrt(Distance2);
				absDistance3 = Math.sqrt(Distance3);
				absDistance4 = Math.sqrt(Distance4);
				double countLoss = 0;
				for(String[] s:collectInfo) {
					if(absDistance1!=0.0)
						countLoss1 = (Math.abs(CountTime(s[2])-CountTime(flightInfo[1])))/(absDistance1);
					if(absDistance2!=0.0)
						countLoss2 = (Math.abs(CountTime(s[3])-CountTime(flightInfo[2])))/(absDistance2);
					if(absDistance3!=0.0)
						countLoss3 = (Math.abs(CountTime(s[5])-CountTime(flightInfo[4])))/(absDistance3);
					if(absDistance4!=0.0)
						countLoss4 = (Math.abs(CountTime(s[6])-CountTime(flightInfo[5])))/(absDistance4);
					countLoss = (countLoss1+countLoss2+countLoss3+countLoss4)/4.0;//ĳ����ϢԴ���е���ʧ
					System.out.println("name:"+s[0]+" loss1: "+countLoss1+"  loss2: "+countLoss2+" loss3: "+countLoss3
								+"  loss4: "+countLoss4);
					tt.setDcon(s[0], countLoss);
				}
				//TODO ���Dcon
				System.out.println("���Dcon");
				for(String s : tt.getDcon().keySet()) {
					//System.out.println(s+" "+tt.getDcon().get(s));
				}
				//TODO ����xRate,yRate
				Map<String,Double> RealityI = tt.getRealityI();//�洢����i��ȷ��
				Map<String,Double> Dcate = tt.getDcate();//
				Map<String,Double> RealityJ = tt.getRealityJ();//�洢����i��ȷ��
				Map<String,Double> Dcon = tt.getDcon();//
				double xRate =0;
				double yRate =0;
				for(String s :RealityI.keySet()) {
					if(!Dcate.containsKey(s)) {
						Dcate.put(s, 0.0);
					}
					if(!Dcon.containsKey(s)) {//���ĳ����ϢԴ�������ṩ��Ϣ�����������ʧ���ó�0
						Dcon.put(s, 0.0);
					}
					xRate += RealityI.get(s) * Dcate.get(s);
					yRate += RealityJ.get(s) * Dcon.get(s);
					
				}
				tt.setXRate(xRate);
				tt.setYRate(yRate);
				//TODO ��֤xRate,yRate����ȷ��
				for(int l=0;l<tt.getXRate().size();l++) {
					//if(Double.isNaN(tt.getYRate().get(l))) {
					System.out.println("xRate:"+tt.getXRate().get(l)+"  "+"yRate:"+tt.getYRate().get(l));
				}
				//�����ݶ��½�����������alpha beta
				double[] theta = new double[2];
				theta = batchGradientDescent();//�����ݶ��½�
				double beta = 1/(theta[0]+1);
				double alpha = theta[0]/(theta[0]+1);
				System.out.println("ԭʼ  alpha: "+alpha+" beta: "+beta);
				//TODO beta��alpha����Ҫ�ۺ�
				beta = 1.0/(1+ Math.pow(Math.E,-beta));
				alpha = 1.0/(1+ Math.pow(Math.E,-alpha));
				System.out.println("sigmoid  alpha: "+alpha+" beta: "+beta);
				double countRealityI;
				double countRealityJ;
				for( String s :tt.getRealityI().keySet()) {
					countRealityI = sigmoid(alpha,s);
					tt.setRealityI(s, countRealityI);
				}
				for( String s :tt.getRealityJ().keySet()) {
					countRealityJ = sigmoid(beta,s);
					tt.setRealityJ(s, countRealityJ);
				}
				tt.clearDcate();
				tt.clearDcon();
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
	
	//��׼ȷ��ת����[0,1]
	public static double sigmoid(double alpha,String s) {
		FCorrectSituation tt = new FCorrectSituation();
		double newReality = 0;
		double result = (alpha+1)*tt.getRealityI().get(s);//���ۼ�
		newReality = 1.0/(1+ Math.pow(Math.E,-result));
		return newReality;
	}
	
	public static double[] batchGradientDescent() {
		double[] theta = {0.1,0.1};
		double[] gradient = {0,0};
		ArrayList<Double> hypothesis = new ArrayList<Double>();
		ArrayList<Double> loss = new ArrayList<Double>();
		ArrayList<Double> cost = new ArrayList<Double>();
		//ArrayList<Double> gradient = new ArrayList<Double>();
		ArrayList<Double> xRate = new ArrayList<Double>();
		ArrayList<Double> yRate = new ArrayList<Double>();
		FCorrectSituation tt = new FCorrectSituation();
		xRate = tt.getXRate();
		yRate = tt.getYRate();
		double costWhole= 100000;
		for(int iter =0;costWhole>0.01 || iter>10000; iter++) {//һֱ��������Ϊֹ���ߵ����趨����ֵ
			//����hypothesis
			double countPerHypothesis;
			for(double a: xRate) {
				countPerHypothesis = a*theta[0]+theta[1];
				hypothesis.add(countPerHypothesis);
			}
			//����loss
			double countPerLoss;
			for(int i=0;i<yRate.size();i++) {
				countPerLoss = hypothesis.get(i) -yRate.get(i);
				loss.add(countPerLoss);
			}
			//����cost
			for(Double a :loss) {
				costWhole += a;
			}
			System.out.println("cost:"+costWhole);
			//����gradient
			double gradient0 = 0;
			double gradient1 = 0;
			for(int i=0;i<yRate.size();i++) {
				gradient0 += xRate.get(i)*loss.get(i); 
				gradient1 += loss.get(i); 
			}
			gradient[0] = gradient0;
			gradient[1] = gradient1;
			//����theta
			theta[0] = theta[0] - alpha * gradient[0];
			theta[1] = theta[1] - alpha * gradient[1];
		}
		return theta;
	}
	
	
	//�������ݼ����ļ������֣�������ݼ��ļ��е����֣���׼�𰸵�λ�ã����ζ�д�ļ��е�λ��
	public static void run(Map<String, String> path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//�ȶ�����ȷ���ı�
		FCorrectSituation tt = new FCorrectSituation();
		
		Configuration conf=FeedbackMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.131:9000");
		FileSystem fs = FileSystem.get(conf);
		
		Path pathSourceNumber = new Path("hdfs://192.168.126.131:9000/user/findTruth/feedback/truth1201/part-r-00000");
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
		
		Job job = new Job(FeedbackMain.config(),"Fstep3");
        String input = path.get("Step3Input");
        String output = path.get("Step3Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(FStep3.class);
        job.setMapperClass(step3Mapper.class);
        job.setReducerClass(step3Reducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		job.waitForCompletion(true);//��ִ����ϣ��˳�
		
	}
}
