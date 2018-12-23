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
			//Step1.run(path);//������׼���ݼ�
		//}
		//Step1.run(path);//������׼���ݼ�
		//Step2.run(path);//�ж���ϢԴ��������Ϣ���Ƿ���ȷ(ʹ��ͶƱ�ķ���)
		//Step3.run(path);//������ϢԴ��ȷ������
		//Step4.run(path);//����ÿ����ϢԴ������
		//Step5.run(path);//step5������ϢԴ��׼ȷ��
		//Step6.run();//�Ӳ����ı��ж�׼ȷ�ʵ��������ʵ���
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
