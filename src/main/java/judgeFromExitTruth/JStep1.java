package judgeFromExitTruth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import voting.TruthMain;

public class JStep1 {
	public static void run() throws IOException {
		//��ȡ��׼������
		
		
		
		
		
		
		
		//���Ҵ�
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		//TODO ������ϢԴ��������ִ��ʮ�飬��������������
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/data/flight_truth/2011-12-01-truth.txt");
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
                    //System.out.println("line="+line);
                    
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
	}
}
