package judgeFromExitTruth;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import voting.TruthMain;

public class JTruthMain {
	public static final String HDFS = "hdfs://192.168.126.130:9000";
	public static void main(String[] args) throws Exception {
		//pathFlight.put("Step1Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-01-data.txt");
		//pathFlight.put("Step1Output", HDFS+"/user/findTruth/JudgeMethod/step1");
		//pathFlight.put("Step2Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-02-data.txt");
		//pathFlight.put("Step2Output", HDFS+"/user/findTruth/JudgeMethod/step1");
		//重新设计pathFlight
		
		Map<String,String> path = new HashMap<String,String>();
		//----------------------step1:产生标准的训练数据集-------------------
		Format f2 = new DecimalFormat("00");
		
		Map<String,String> pathFlight = new HashMap<String,String>();
		for(int i = 1;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			pathFlight.put("Step"+String.valueOf(i)+"Input", 
					HDFS+"/user/findTruth/data/clean_flight/2011-12-"+f2.format(i)+"-data.txt");
			pathFlight.put("Step"+String.valueOf(i)+"Output", HDFS+"/user/findTruth/JudgeMethod1/step1/"+String.valueOf(i));
			}
		}
		//验证训练集的名称
		//for(String s : pathFlight.keySet()) {
			//System.out.printf("%s             %s\n",s,pathFlight.get(s));
		//}
		for(int i=1 ;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			System.out.println("轮次"+i);
			JStep1.run("Step"+String.valueOf(i)+"Input","Step"+String.valueOf(i)+"Output",pathFlight);//产生标准的训练数据集
			}
		}
		
		//----------------------step2:产生每个月的答案-------------------
		Map<String,String> pathTruth = new HashMap<String,String>();
		for(int i = 1;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
				pathTruth.put("Step"+String.valueOf(i)+"Input", 
					HDFS+"/user/findTruth/data/flight_truth/2011-12-"+f2.format(i)+"-truth.txt");
				pathTruth.put("Step"+String.valueOf(i)+"Output", HDFS+"/user/findTruth/JudgeMethod1/step2/"+String.valueOf(i));
			}
		}
		//验证训练集的名称
		//for(String s : pathFlight.keySet()) {
			//System.out.printf("%s             %s\n",s,pathFlight.get(s));
		//}
		
		for(int i=1 ;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			System.out.println("轮次"+i);
			JStep2.run("Step"+String.valueOf(i)+"Input","Step"+String.valueOf(i)+"Output",pathTruth);//产生标准的训练数据集
			}
		}
		
		//-------------------------step3:计算每个信息源的数量--------------------------------
		
		path.put("Step3Input", HDFS+"/user/findTruth/JudgeMethod1/step1");
		path.put("Step3Output",HDFS+"/user/findTruth/JudgeMethod1/step3" );
		JStep3.run(path);
		
		//-------------------------step4:计算正确的信息个数--------------------------------
		//拿标准的训练集和测试集比较，把正确的信息源记下来
		Map<String,String> step4Path = new HashMap<String,String>();//标准答案的文件路径
		for(int i = 1;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
				step4Path.put("Step"+String.valueOf(i)+"Input", 
					HDFS+"/user/findTruth/JudgeMethod/step1/"+String.valueOf(i));
				step4Path.put("Step"+String.valueOf(i)+"Output", HDFS+"/user/findTruth/JudgeMethod1/step4/"+String.valueOf(i));
			}
		}
		for(int i=1 ;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			System.out.println("轮次"+i);
			JStep4.run("Step"+String.valueOf(i)+"Input","Step"+String.valueOf(i)+"Output",pathTruth,step4Path);//产生标准的训练数据集
			}
		}
		//step4Path.put("Step1Output",HDFS+"/user/findTruth/JudgeMethod/step4/1/1");只取1月测试
		//JStep4.run("Step1Input","Step1Output",pathTruth,step4Path);
		
		//------------------------step5:总汇正确数据源的数量--------------------------------
		path.put("Step5Input", HDFS+"/user/findTruth/JudgeMethod1/step4");
		path.put("Step5Output",HDFS+"/user/findTruth/JudgeMethod1/step5" );
		JStep5.run(path);
		
		//-----------------------step6:计算正确率-------------------------------------------
		JStep6.run(path);
		
		//-----------------------step7:写入<判断的情况，实际的情况>-------------------------------------------
		path.put("Step7Input", HDFS+"/user/findTruth/data/standard_test_flight");
		path.put("Step7Output", HDFS+"/user/findTruth/JudgeMethod1/step7");
		JStep7.run(path);
		
		//-----------------------step8:统计测验集正确和错误的情况-------------------------------------------
		path.put("Step8Input", path.get("Step7Output"));
		path.put("Step8Output", HDFS+"/user/findTruth/JudgeMethod1/step8");
		JStep8.run(path);
		
		//----------------------step9:输出正确率-----------------------------------------------------------
		JStep9.run(path);
		
		
		System.out.println("main");
		//Jstep5从测试文本判断准确率的情况和真实情况
		System.exit(0);
	}
	
    public static JobConf config() {
        JobConf conf = new JobConf(TruthMain.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    
}
