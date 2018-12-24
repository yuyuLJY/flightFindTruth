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
		//�������pathFlight
		
		Map<String,String> path = new HashMap<String,String>();
		//----------------------step1:������׼��ѵ�����ݼ�-------------------
		Format f2 = new DecimalFormat("00");
		
		Map<String,String> pathFlight = new HashMap<String,String>();
		for(int i = 1;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			pathFlight.put("Step"+String.valueOf(i)+"Input", 
					HDFS+"/user/findTruth/data/clean_flight/2011-12-"+f2.format(i)+"-data.txt");
			pathFlight.put("Step"+String.valueOf(i)+"Output", HDFS+"/user/findTruth/JudgeMethod/step1/"+String.valueOf(i));
			}
		}
		//��֤ѵ����������
		//for(String s : pathFlight.keySet()) {
			//System.out.printf("%s             %s\n",s,pathFlight.get(s));
		//}
		for(int i=1 ;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			System.out.println("�ִ�"+i);
			JStep1.run("Step"+String.valueOf(i)+"Input","Step"+String.valueOf(i)+"Output",pathFlight);//������׼��ѵ�����ݼ�
			}
		}
		
		//----------------------step2:����ÿ���µĴ�-------------------
		Map<String,String> pathTruth = new HashMap<String,String>();
		for(int i = 1;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
				pathTruth.put("Step"+String.valueOf(i)+"Input", 
					HDFS+"/user/findTruth/data/flight_truth/2011-12-"+f2.format(i)+"-truth.txt");
				pathTruth.put("Step"+String.valueOf(i)+"Output", HDFS+"/user/findTruth/JudgeMethod/step2/"+String.valueOf(i));
			}
		}
		//��֤ѵ����������
		//for(String s : pathFlight.keySet()) {
			//System.out.printf("%s             %s\n",s,pathFlight.get(s));
		//}
		
		for(int i=1 ;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			System.out.println("�ִ�"+i);
			JStep2.run("Step"+String.valueOf(i)+"Input","Step"+String.valueOf(i)+"Output",pathTruth);//������׼��ѵ�����ݼ�
			}
		}
		
		//-------------------------step3:����ÿ����ϢԴ������--------------------------------
		
		path.put("Step3Input", HDFS+"/user/findTruth/JudgeMethod/step1");
		path.put("Step3Output",HDFS+"/user/findTruth/JudgeMethod/step3" );
		JStep3.run(path);
		
		//-------------------------step4:������ȷ����Ϣ����--------------------------------
		//�ñ�׼��ѵ�����Ͳ��Լ��Ƚϣ�����ȷ����ϢԴ������
		Map<String,String> step4Path = new HashMap<String,String>();//��׼�𰸵��ļ�·��
		for(int i = 1;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
				step4Path.put("Step"+String.valueOf(i)+"Input", 
					HDFS+"/user/findTruth/JudgeMethod/step1/"+String.valueOf(i));
				step4Path.put("Step"+String.valueOf(i)+"Output", HDFS+"/user/findTruth/JudgeMethod/step4/"+String.valueOf(i));
			}
		}
		for(int i=1 ;i<=26;i++) {
			if(i!=6  && i!=21 && i !=23) {
			System.out.println("�ִ�"+i);
			JStep4.run("Step"+String.valueOf(i)+"Input","Step"+String.valueOf(i)+"Output",pathTruth,step4Path);//������׼��ѵ�����ݼ�
			}
		}
		//step4Path.put("Step1Output",HDFS+"/user/findTruth/JudgeMethod/step4/1/1");ֻȡ1�²���
		//JStep4.run("Step1Input","Step1Output",pathTruth,step4Path);
		
		//------------------------step5:�ܻ���ȷ����Դ������--------------------------------
		path.put("Step5Input", HDFS+"/user/findTruth/JudgeMethod/step4");
		path.put("Step5Output",HDFS+"/user/findTruth/JudgeMethod/step5" );
		JStep5.run(path);
		
		//-----------------------step6:������ȷ��-------------------------------------------
		JStep6.run(path);
		
		//-----------------------step7:д��<�жϵ������ʵ�ʵ����>-------------------------------------------
		path.put("Step7Input", HDFS+"/user/findTruth/data/standard_test_flight");
		path.put("Step7Output", HDFS+"/user/findTruth/JudgeMethod/step7");
		JStep7.run(path);
		
		//-----------------------step8:ͳ�Ʋ��鼯��ȷ�ʹ�������-------------------------------------------
		path.put("Step8Input", path.get("Step7Output"));
		path.put("Step8Output", HDFS+"/user/findTruth/JudgeMethod/step8");
		JStep8.run(path);
		
		//----------------------step9:�����ȷ��-----------------------------------------------------------
		JStep9.run(path);
		
		
		System.out.println("main");
		//Jstep5�Ӳ����ı��ж�׼ȷ�ʵ��������ʵ���
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
