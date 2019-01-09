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
	//按照航班把信息分类	
	public static class step3Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\t");//tap符号
			String[] splitInformation = splitResult[1].split("/t");
			//System.out.printf("map %s\n",Arrays.toString(splitResult));
			//System.out.printf("map %s\n",Arrays.toString(splitInformation));
			context.write(new Text(splitInformation[1]), new Text(splitResult[1]));//<"MP190",整条信息>
		}
	}
	
	//把正确的信息源写成： <"travel",1>
	public static class step3Reducer extends Reducer<Text,Text,Text,IntWritable> {
		 IntWritable one = new IntWritable(1);  //定义输出值始终是1
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			FCorrectSituation tt = new FCorrectSituation();
			if(tt.getCorrecInfo().containsKey(key.toString())) {//答案里边有
				 System.out.println("验证Dcon的大小："+tt.getDcon().size());
				 String[] flightInfo = tt.getCorrecInfo().get(key.toString());//找到南航AT900的正确信息
				 ArrayList<String []> collectInfo = new ArrayList<String[]>(); //把38个信息源给出的航班信息收集起来
				 //------------------验证flightInfo-----------------------------
				 System.out.printf("正确情况： %s\n",Arrays.toString(flightInfo));
				 for(Text value : values) {
					 double IsDepartureGateCorrect = 1;
					 double IsArriveGateCorrect = 1;
					//-------------------实际的情况---------------
					 String[] saveSplitResult = value.toString().split("/t");//分割
					 collectInfo.add(saveSplitResult);//把每条信息都存起来
					 //System.out.printf("信息源 %s\n",Arrays.toString(saveSplitResult));
					 //判断登机口和到达口
					 if(!saveSplitResult[4].equals("0") && !flightInfo[3].equals("0")&& !saveSplitResult[4].equals(flightInfo[3])) {//判断登机口
						 IsDepartureGateCorrect = 0;
					 }
					 if(!saveSplitResult[7].equals("0") && !flightInfo[6].equals("0")&&!saveSplitResult[7].equals(flightInfo[6])) {//判断到达口
						 IsArriveGateCorrect = 0;
					 }
					 tt.setDcate(saveSplitResult[0], IsArriveGateCorrect+IsDepartureGateCorrect);
				 }
				//--------------------收集信息完整后-----------------
				double sumTime1=0,sumTime2 = 0,sumTime3=0,sumTime4 = 0;//所有信息源的列时间和
				double AveTime1=0,AveTime2 = 0,AveTime3=0,AveTime4 = 0;//所有信息源所有信息源的列时间平均值
				double Distance1=0,Distance2 = 0,Distance3=0,Distance4 = 0;//列信息的平方差
				double absDistance1=0,absDistance2 = 0,absDistance3=0,absDistance4 = 0;//绝对值距离
				double countLoss1=0,countLoss2 = 0,countLoss3=0,countLoss4 = 0;//
				double number = collectInfo.size();//一共有多少个信息源参与计算
				//计算每列的总和
				for(String[] s:collectInfo) {
					sumTime1 += CountTime(s[2]);
					sumTime2 += CountTime(s[3]);
					sumTime3 += CountTime(s[5]);
					sumTime4 += CountTime(s[6]);
				}
				//计算平均值
				AveTime1 = sumTime1/number;
				AveTime2 = sumTime2/number;
				AveTime3 = sumTime3/number;
				AveTime4 = sumTime4/number;
				//计算con分母
				for(String[] s:collectInfo) {
					Distance1 +=  Math.pow(CountTime(s[2])-AveTime1,2);
					Distance2 +=  Math.pow(CountTime(s[3])-AveTime2,2);
					Distance3 +=  Math.pow(CountTime(s[5])-AveTime3,2);
					Distance4 +=  Math.pow(CountTime(s[6])-AveTime4,2);
					System.out.println("Distance1:"+Distance1+"Distance2:"+Distance2+"Distance3:"+Distance3+"Distance4:"+Distance4);
				}
				//计算分母开根号以后的值
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
					countLoss = (countLoss1+countLoss2+countLoss3+countLoss4)/4.0;//某个信息源四列的损失
					System.out.println("name:"+s[0]+" loss1: "+countLoss1+"  loss2: "+countLoss2+" loss3: "+countLoss3
								+"  loss4: "+countLoss4);
					tt.setDcon(s[0], countLoss);
				}
				//TODO 检查Dcon
				System.out.println("检查Dcon");
				for(String s : tt.getDcon().keySet()) {
					//System.out.println(s+" "+tt.getDcon().get(s));
				}
				//TODO 插入xRate,yRate
				Map<String,Double> RealityI = tt.getRealityI();//存储本次i正确率
				Map<String,Double> Dcate = tt.getDcate();//
				Map<String,Double> RealityJ = tt.getRealityJ();//存储本次i正确率
				Map<String,Double> Dcon = tt.getDcon();//
				double xRate =0;
				double yRate =0;
				for(String s :RealityI.keySet()) {
					if(!Dcate.containsKey(s)) {
						Dcate.put(s, 0.0);
					}
					if(!Dcon.containsKey(s)) {//如果某个信息源不参与提供信息，则把他的损失设置成0
						Dcon.put(s, 0.0);
					}
					xRate += RealityI.get(s) * Dcate.get(s);
					yRate += RealityJ.get(s) * Dcon.get(s);
					
				}
				tt.setXRate(xRate);
				tt.setYRate(yRate);
				//TODO 验证xRate,yRate的正确性
				for(int l=0;l<tt.getXRate().size();l++) {
					//if(Double.isNaN(tt.getYRate().get(l))) {
					System.out.println("xRate:"+tt.getXRate().get(l)+"  "+"yRate:"+tt.getYRate().get(l));
				}
				//调用梯度下降，计算合理的alpha beta
				double[] theta = new double[2];
				theta = batchGradientDescent();//调用梯度下降
				double beta = 1/(theta[0]+1);
				double alpha = theta[0]/(theta[0]+1);
				System.out.println("原始  alpha: "+alpha+" beta: "+beta);
				//TODO beta和alpha还需要折合
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
	
	//从日期形式，计算出出发时间
	public static int CountTime(String s){
		  //System.out.printf("输入字符串：%s\n",s);
		  String actualTime = "0";
		  String[] split = s.split(" ");//把出发时间分片
		  for(String t : split) {//找到包含":"的时间,但是不要括号里面的
			  if(t.contains(":") && !t.contains("(")) {
				  actualTime = t;
				  actualTime = actualTime.replaceAll(" ", "");//先去除空白符号
				  actualTime = actualTime.replaceAll("[a-zA-Z]", "");
				  //TODO something wrong
				  //+、*、|、\等符号在正则表达示中有相应的不同意义
				  actualTime = actualTime.replaceAll("\\*", "");
			  }
		  }
		  //System.out.printf("找到的到达时间  %s\n",actualTime);
		  int CountTime =0;
		  if(actualTime.contains(":")) {
			  String[] splitCountTime = actualTime.split(":");
			  CountTime = Integer.parseInt(splitCountTime[0])*60+Integer.parseInt(splitCountTime[1]);  
		  }
		  return CountTime;
	}
	
	//把准确率转化成[0,1]
	public static double sigmoid(double alpha,String s) {
		FCorrectSituation tt = new FCorrectSituation();
		double newReality = 0;
		double result = (alpha+1)*tt.getRealityI().get(s);//有累加
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
		for(int iter =0;costWhole>0.01 || iter>10000; iter++) {//一直做到降低为止或者到了设定的阈值
			//计算hypothesis
			double countPerHypothesis;
			for(double a: xRate) {
				countPerHypothesis = a*theta[0]+theta[1];
				hypothesis.add(countPerHypothesis);
			}
			//计算loss
			double countPerLoss;
			for(int i=0;i<yRate.size();i++) {
				countPerLoss = hypothesis.get(i) -yRate.get(i);
				loss.add(countPerLoss);
			}
			//计算cost
			for(Double a :loss) {
				costWhole += a;
			}
			System.out.println("cost:"+costWhole);
			//计算gradient
			double gradient0 = 0;
			double gradient1 = 0;
			for(int i=0;i<yRate.size();i++) {
				gradient0 += xRate.get(i)*loss.get(i); 
				gradient1 += loss.get(i); 
			}
			gradient[0] = gradient0;
			gradient[1] = gradient1;
			//计算theta
			theta[0] = theta[0] - alpha * gradient[0];
			theta[1] = theta[1] - alpha * gradient[1];
		}
		return theta;
	}
	
	
	//输入数据集的文件夹名字，输出数据集文件夹的名字，标准答案的位置，本次读写文件夹的位置
	public static void run(Map<String, String> path) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//先读入正确的文本
		FCorrectSituation tt = new FCorrectSituation();
		
		Configuration conf=FeedbackMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.131:9000");
		FileSystem fs = FileSystem.get(conf);
		
		Path pathSourceNumber = new Path("hdfs://192.168.126.131:9000/user/findTruth/feedback/truth1201/part-r-00000");
		tt.getCorrecInfo().clear();//!!!!!统计每个月的时候，要清除
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Exists!");
			try {
				//此为hadoop读取数据类型
				FSDataInputStream is = fs.open(pathSourceNumber);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	String[] splitValue = split[1].split("/t"); 
                	tt.setCorrectInfo(split[0],splitValue);//写信息源的情况
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
		
		//----------------------------验证读入正确
		//Map<String,String[]> correcInfo = tt.getCorrecInfo();
		//System.out.println("验证正确信息");
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
		job.waitForCompletion(true);//若执行完毕，退出
		
	}
}
