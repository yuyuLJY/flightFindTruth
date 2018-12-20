package com.baidu;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class Voting {
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
				System.out.printf("�ָ�������%s\n",Arrays.toString(saveSplitResult));
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
					strSplitSplit = saveSplitResult[i].split(" ");//�ж���������ǲ�������
					//i =2
					if((j==2 || j==3)) {
						if(strSplitSplit.length>=2 || saveSplitResult[i].equals("") ) {//�пո�
							if(strSplitSplit.length>=2) {
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}else {//����Ϊ1
							j=4;//��һ�ִ�5��ʼ
							standardEightColumn[j] = saveSplitResult[i];
						}
					}
					if(j==4 && strSplitSplit.length==1) {//""����FD8
						if(saveSplitResult[i].equals("")==false) {//ֻ����ʵ��ֵ�ŻḲ��"0"
							standardEightColumn[j] = saveSplitResult[i];
						}
						i++;
					}//�ָ�������λ�ģ�j���ƣ�i����
					if(j==5 || j==6) {
						//System.out.printf("��ǰ��%s\n",Arrays.toString(strSplitSplit));
						if(strSplitSplit.length>=2 || saveSplitResult[i].equals("") ) {//�пո�
							if(strSplitSplit.length>=2) {
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}else {//����Ϊ1
							j=7;
							standardEightColumn[j] = saveSplitResult[i];
						}
					}
					if(j==7) {
						if(!saveSplitResult[i].equals("")) {
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
			for(int i=0;i<flightData.size();i++) {
				System.out.printf("��7�������%s\n",flightData.get(i)[7]);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
