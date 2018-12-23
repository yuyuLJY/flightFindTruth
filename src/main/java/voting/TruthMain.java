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
		path.put("Step1Input", HDFS+"/user/findTruth/data/test_flight/2011-12-27-data.txt");
		//path.put("Step1Input", HDFS+"/user/findTruth/data/test_flight");
		path.put("Step1Output", HDFS+"/user/findTruth/data/standard_test_flight");
		path.put("Step2Input",path.get("Step1Output"));
		path.put("Step2Output",HDFS+"/user/findTruth/step2");
		path.put("Step3Input", path.get("Step2Output"));
		path.put("Step3Output",HDFS+"/user/findTruth/step3" );
		path.put("Step4Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-01-data.txt");
		path.put("Step4Output",HDFS+"/user/findTruth/step4");
		path.put("Step6Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-27-data.txt");
		path.put("Step6Input", HDFS+"/user/findTruth/step6/1");
		
		Format f2 = new DecimalFormat("00");
		//System.out.println("---");
		//for(int i=1;i<26;i++) {
			//path.put("Step1Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-"+f2.format(i)+"-data.txt");
			//Step1.run(path);//产生标准数据集
		//}
		//Step1.run(path);//产生标准数据集
		//Step2.run(path);//判断信息源的四条信息，是否正确(使用投票的方法)
		//Step3.run(path);//计算信息源正确的总数
		//Step4.run(path);//计算每个信息源的数量
		//Step5.run(path);//step5计算信息源的准确率
		//Step6.run();//从测试文本判断准确率的情况和真实情况
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
