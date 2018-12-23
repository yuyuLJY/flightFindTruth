package judgeFromExitTruth;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import voting.TruthMain;

public class JTruthMain {
	public static final String HDFS = "hdfs://192.168.126.130:9000";
	public static void main(String[] args) throws Exception {
		Map<String,String> path = new HashMap<String,String>();
		String name = "/user/findTruth";
		//path.put("Step1Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-01-data.txt");
		//path.put("Step1Output", HDFS+"/user/findTruth/data/standard_clean_flight");
		
		//Step1.run(path);//产生标准数据集(跟投票方法一样)，已经产生了，不需要再去执行step1
		JStep1.run();//读入正确的答案
		//Step3.run(path);//判断信息源的四条信息，是否正确(使用对比正确信息的方法)
		//Step4.run(path);//计算每个信息源的数量
		//Step5.run(path);//step5计算信息源的准确率
		//step6从测试文本判断准确率的情况和真实情况
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
