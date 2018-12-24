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
		//Step1.run(path);//������׼���ݼ�
		//Step2.run(path);//�ж���ϢԴ��������Ϣ���Ƿ���ȷ(ʹ��ͶƱ�ķ���)
		//Step3.run(path);//������ϢԴ��ȷ������
		//Step4.run(path);//����ÿ����ϢԴ������
		//Step5.run(path);//step5������ϢԴ��׼ȷ��
		//Step6.run(path);//��׼����ʵ���ݼ�filght_truth
		//Step7.run(path);//�Ӳ����ı��ж�׼ȷ�ʵ��������ʵ���
		//Step8.run(path);//ͳ����ȷ�����
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
