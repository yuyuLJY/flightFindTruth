package feedbackMethod;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import voting.TruthMain;

public class FeedbackMain {
	public static final String HDFS = "hdfs://192.168.126.131:9000";
	public static void main(String[] args) throws Exception {
		
		Map<String,String> path = new HashMap<String,String>();
		//----------------------step1:产生标准的训练数据集-------------------
		path.put("StepIterInput", HDFS+"/user/findTruth/feedback/data1201");
		path.put("StepIterOutput", HDFS+"/user/findTruth/feedback/countIter/1");
		//path.put("Step1Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-01-data.txt");//输入标准的数据集
		//path.put("Step1Output", HDFS+"/user/findTruth/feedback/data1201");
		//path.put("Step2Input", HDFS+"/user/findTruth/data/flight_truth/2011-12-01-truth.txt");
		//path.put("Step2Output", HDFS+"/user/findTruth/feedback/truth1201");//输出标准的答案集
		path.put("Step3Input", HDFS+"/user/findTruth/feedback/data1201");//输入标准的数据集
		path.put("Step3Output", HDFS+"/user/findTruth/feedback/step3/54");
		path.put("Step4Input", HDFS+"/user/findTruth/feedback/data/TestData1211");
		path.put("Step4Output", HDFS+"/user/findTruth/feedback/Step4/4");
		
		//TODO 先把正确率先写进去
		FCorrectSituation tt = new FCorrectSituation();
		String[] sourceName = 
			{"boston","mia","quicktrip","helloflight","flightexplorer","den","ua","iad","allegiantair",
			"panynj","mco","weather","flightarrival","ifly","aa","ord","flightaware","myrateplan","flightview","flightstats",
			"flightwise","flylouisville","airtravelcenter","mytripandmore","CO","usatoday","world-flight-tracker","orbitz","businesstravellogue",
			"dfw","flytecomm","phl","wunderground","gofox","travelocity","flights","sfo","foxbusiness"};//创建数据源的名称集合
		for(int i=0;i<sourceName.length;i++) {
			tt.setRealityI(sourceName[i], 1.0);
			tt.setRealityJ(sourceName[i], 1.0);
		}
	
		//FStep1.run(path);//产生标准数据集
		//FStep2.run(path);//产生标准答案集
		FStep3.run(path);//计算迭代的正确率
		FStep4.run(path);
		//FindNumberOfIter.run(path);
		//System.out.println("迭代次数： "+tt.getIter());
		//验证准去率
		for(String s: tt.getRealityI().keySet()) {
			System.out.println("name:"+s+" "+tt.getRealityI().get(s)+" "+tt.getRealityJ().get(s));
		}
		System.out.println("main");
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
