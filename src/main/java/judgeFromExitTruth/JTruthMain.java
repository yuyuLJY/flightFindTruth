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
		
		//Step1.run(path);//������׼���ݼ�(��ͶƱ����һ��)���Ѿ������ˣ�����Ҫ��ȥִ��step1
		JStep1.run();//������ȷ�Ĵ�
		//Step3.run(path);//�ж���ϢԴ��������Ϣ���Ƿ���ȷ(ʹ�öԱ���ȷ��Ϣ�ķ���)
		//Step4.run(path);//����ÿ����ϢԴ������
		//Step5.run(path);//step5������ϢԴ��׼ȷ��
		//step6�Ӳ����ı��ж�׼ȷ�ʵ��������ʵ���
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
