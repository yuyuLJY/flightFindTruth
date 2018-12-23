package voting;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class NewVoting {
	static final int standardColumnsNumber = 8;
	public static void main(String[] args) {
		try {
			String name = "src/main/text/2011-12-01-data.txt";
			File file = new File(name);
			InputStreamReader input = new InputStreamReader(new FileInputStream(file));
			BufferedReader bf = new BufferedReader(input);
			String str;//�н�ÿ�е�String
			String[] saveSplitResult;
			ArrayList<String []> flightData = new ArrayList<String []>();
			String[] strSplitSplit;
			while((str = bf.readLine())!=null) {
				saveSplitResult = str.split("\t");
				//System.out.printf("�ָ������鳤��%d %s\n",saveSplitResult.length,Arrays.toString(saveSplitResult));
				String[] standardEightColumn = new String [standardColumnsNumber]; 
				//TODO ��ʼȫΪ-1
				for(int ii=0;ii<standardColumnsNumber;ii++) {
					standardEightColumn[ii]="0";
				}
				//TODO ��������������8����Ϣ
				int i = 0;//i�Ƿָ�õ�����saveSplitResult������
				for(int j=0; j<standardColumnsNumber; j++) {//ʹ�÷ָ�õ�����ȥ����׼������
					//i=0 || i=1
					if(i>=saveSplitResult.length) {
						break;
					}
					if(j<=1) {
						standardEightColumn[j] = saveSplitResult[i];
						i++;
					}
					//System.out.printf("j:%d i:%d %s\n",j,i,saveSplitResult[i]);
					//strSplitSplit = saveSplitResult[i].split(" ");//�ж���������ǲ�������
					//i =2
					if((j==2 || j==3)) {
						if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline")) {//�пո�
							if(saveSplitResult[i].contains(":")) {//��ð�ž�������
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}else {//����Ϊ1
							j=4;//��һ�ִ�5��ʼ
							standardEightColumn[j] = saveSplitResult[i];
						}
					}
					//System.out.printf("%d %s\n",j,Arrays.toString(strSplitSplit));
					if(j==4 &&(!saveSplitResult[i].contains(":"))) {//""����FD8
						if(saveSplitResult[i].equals("")==false && 
								(!saveSplitResult[i].contains("Not provided by airline"))) {//ֻ����ʵ��ֵ�ŻḲ��"0"
							standardEightColumn[j] = saveSplitResult[i];
						}
						i++;
					}//�ָ�������λ�ģ�j���ƣ�i����
					if(j==5 || j==6) {
						//System.out.printf("��ǰ��%s\n",Arrays.toString(strSplitSplit));
						if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline") ) {//�пո�
							if(saveSplitResult[i].contains(":")) {
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}else {//����Ϊ1
							j=7;
							standardEightColumn[j] = saveSplitResult[i];
						}
					}
					if(j==7) {
						if(!saveSplitResult[i].equals("") && 
								(!saveSplitResult[i].contains("Not provided by airline"))) {
							standardEightColumn[j] = saveSplitResult[i];
						}
						i++;
					}
					//System.out.printf("��ǰ���%d��%s\n",j,Arrays.toString(standardEightColumn));
				}
				//�������һ�е���Ϣ����ӽ�Arraylist
				//System.out.printf("%s %s\n",standardEightColumn[0],standardEightColumn[1]);
				flightData.add(standardEightColumn);
			}
			bf.close();
			input.close();
			//����������ڣ���2��
			for(int i=0;i<flightData.size();i++) {
				System.out.printf("��7�������%d�У�%-25s %-25s\n",i,flightData.get(i)[2],flightData.get(i)[3]);
				//����" "�ָ
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
