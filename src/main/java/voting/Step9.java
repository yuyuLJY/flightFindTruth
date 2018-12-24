package voting;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Step9 {
	public static void run(Map<String, String> path) throws IOException {
		Configuration conf=TruthMain.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.130:9000");
		FileSystem fs = FileSystem.get(conf);
		
		
		//TODO ������ϢԴ��������ִ��ʮ�飬��������������
		Path pathSourceNumber = new Path("hdfs://192.168.126.130:9000/user/findTruth/Voting/step8/part-r-00000");
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
                int wrongNumber = 0;
                int correctNumber = 0;
                while((line=reader.readLine())!=null){
                	String[] split = line.split("\t");
                    System.out.printf("%s: \n",Arrays.toString(split));//[0, 0, 458]
                    if(split[0].equals(split[1])) {//��ȷ��
                    	correctNumber += Integer.parseInt(split[2]);
                    }else {
                    	wrongNumber += Integer.parseInt(split[2]);
                    }
                }
                //������ȷ��
                float wholeCorrectRate = (float)correctNumber/((float)(wrongNumber+correctNumber));
                System.out.printf("����������ȷ��Ϊ��%f",wholeCorrectRate);
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("������");
		}
	}
}
