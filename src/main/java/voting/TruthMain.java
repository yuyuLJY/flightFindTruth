package voting;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

public class TruthMain {
	public static final String HDFS = "hdfs://192.168.126.130:9000";
	public static void main(String[] args) throws Exception {
		Map<String,String> path = new HashMap<String,String>();
		String name = "/user/findTruth";
		path.put("Step1Input", HDFS+"/user/findTruth/data/clean_flight");
		//path.put("Step1Input", HDFS+"/user/findTruth/data/test_flight");
		path.put("Step1Output", HDFS+"/user/findTruth/data/standard_clean_flight");
		path.put("Step2Input",path.get("Step1Output"));
		path.put("Step2Output",HDFS+"/user/findTruth/Voting/step2");
		path.put("Step3Input", path.get("Step2Output"));
		path.put("Step3Output",HDFS+"/user/findTruth/Voting/step3" );
		path.put("Step4Input", HDFS+"/user/findTruth/data/standard_clean_flight");
		path.put("Step4Output",HDFS+"/user/findTruth/Voting/step4");
		path.put("Step6Input", HDFS+"/user/findTruth/data/flight_truth/2011-12-27-truth.txt");
		path.put("Step6Output", HDFS+"/user/findTruth/data/standard_truth_flight");
		path.put("Step7Input", HDFS+"/user/findTruth/data/standard_test_flight");
		path.put("Step7Output", HDFS+"/user/findTruth/Voting/step7");			
		path.put("Step8Input", path.get("Step7Output"));
		path.put("Step8Output", HDFS+"/user/findTruth/Voting/step8");
		//Format f2 = new DecimalFormat("00");
		//Step1.run(path);//产生标准数据集
		//Step2.run(path);//判断信息源的四条信息，是否正确(使用投票的方法)
		//Step3.run(path);//计算信息源正确的总数
		//Step4.run(path);//计算每个信息源的数量
		//Step5.run(path);//step5计算信息源的准确率
		//Step6.run(path);//标准化真实数据集filght_truth
		//Step7.run(path);//从测试文本判断准确率的情况和真实情况
		//Step8.run(path);//统计正确的情况
		Step9.run(path);
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
