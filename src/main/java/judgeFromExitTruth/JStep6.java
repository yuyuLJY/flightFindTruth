package judgeFromExitTruth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import voting.CorrectSituation;
import voting.TruthMain;

public class JStep6 {
	public static void run(Map<String, String> path) throws IOException {
		CorrectSituation tt = new CorrectSituation();
		
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//������ϢԴ������
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/JudgeMethod/step3/part-r-00000");
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Exists!");
			try {
				//��Ϊhadoop��ȡ��������
				FSDataInputStream is = fs.open(pathSourceNumber);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //�����ݶ��뵽��������
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //�ӻ������ж�ȡ����
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	tt.setSourceNumber(split[0], Integer.parseInt(split[1]));//д��ϢԴ������
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
		
		//������ϢԴ��ȷ������
		Path pathCorrect = new Path("hdfs://192.168.126.130:9000/user/findTruth/JudgeMethod/step5/part-r-00000");
		if (fs.exists(pathCorrect)) {
			System.out.println("Exists!");
			try {
				//��Ϊhadoop��ȡ��������
				FSDataInputStream is = fs.open(pathCorrect);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //�����ݶ��뵽��������
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //�ӻ������ж�ȡ����
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                	tt.setCorrectNumber(split[0], Integer.parseInt(split[1]));//д��ϢԴ������
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
		
		
		//------------------------��֤�Ƿ���ȷ------------------------
		Map<String,Integer> sourceNumber = tt.getSourceNumber();
		System.out.println("��֤��ϢԴ�ĸ���");
		for(String s : sourceNumber.keySet()) {
			System.out.printf("%s %d\n",s,sourceNumber.get(s));
		}
		
		Map<String,Integer> sourceCorrectNumber  = tt.getCorrectNumber();
		System.out.println("��֤��ȷ�ĸ���");
		for(String s : sourceCorrectNumber.keySet()) {
			System.out.printf("%s %d\n",s,sourceCorrectNumber.get(s));
		}
		
		//-----------------------------������ȷ��-------------------------
		int up = 0;
		int down = 0;
		for(String s : sourceNumber.keySet()) {
			float correctRate = 0;
			System.out.printf("%s    %d %d\n",s,sourceCorrectNumber.get(s),sourceNumber.get(s));
			if(sourceCorrectNumber.containsKey(s)) {
				up = sourceCorrectNumber.get(s);
			}
			if(sourceNumber.containsKey(s)) {
				down = sourceNumber.get(s);
			}
			correctRate = (float)up/(float)(down);
			tt.setCorrectRate(s, correctRate);
		}
		
		//---------------------------��֤��ȷ��-------------------
		Map<String,Float> correctRateMap  = tt.getCorrectRate();
		System.out.println("��֤׼ȷ��");
		for(String s : correctRateMap.keySet()) {
			System.out.printf("%s	%f\n",s,correctRateMap.get(s));
		}
		
		//-----------------��׼ȷ��д��hdfs�ļ�------------
		Path CorrectRatePath = new Path("hdfs://192.168.126.130:9000/user/findTruth/step5/1");
		FSDataOutputStream outStream=fs.create(CorrectRatePath);
		
	}
}
