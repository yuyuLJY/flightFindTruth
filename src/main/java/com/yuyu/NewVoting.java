package com.yuyu;

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
			String str;//承接每行的String
			String[] saveSplitResult;
			ArrayList<String []> flightData = new ArrayList<String []>();
			String[] strSplitSplit;
			while((str = bf.readLine())!=null) {
				saveSplitResult = str.split("\t");
				//System.out.printf("分割后的数组长度%d %s\n",saveSplitResult.length,Arrays.toString(saveSplitResult));
				String[] standardEightColumn = new String [standardColumnsNumber]; 
				//TODO 初始全为-1
				for(int ii=0;ii<standardColumnsNumber;ii++) {
					standardEightColumn[ii]="0";
				}
				//TODO 处理填充成完整的8列信息
				int i = 0;//i是分割好的数组saveSplitResult的索引
				for(int j=0; j<standardColumnsNumber; j++) {//使用分割得到的行去填充标准的数组
					//i=0 || i=1
					if(i>=saveSplitResult.length) {
						break;
					}
					if(j<=1) {
						standardEightColumn[j] = saveSplitResult[i];
						i++;
					}
					//System.out.printf("j:%d i:%d %s\n",j,i,saveSplitResult[i]);
					//strSplitSplit = saveSplitResult[i].split(" ");//判断这个数据是不是日期
					//i =2
					if((j==2 || j==3)) {
						if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline")) {//有空格
							if(saveSplitResult[i].contains(":")) {//有冒号就是日期
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}else {//长度为1
							j=4;//下一轮从5开始
							standardEightColumn[j] = saveSplitResult[i];
						}
					}
					//System.out.printf("%d %s\n",j,Arrays.toString(strSplitSplit));
					if(j==4 &&(!saveSplitResult[i].contains(":"))) {//""或者FD8
						if(saveSplitResult[i].equals("")==false && 
								(!saveSplitResult[i].contains("Not provided by airline"))) {//只有真实的值才会覆盖"0"
							standardEightColumn[j] = saveSplitResult[i];
						}
						i++;
					}//分割结果是两位的：j下移，i不动
					if(j==5 || j==6) {
						//System.out.printf("当前列%s\n",Arrays.toString(strSplitSplit));
						if(saveSplitResult[i].contains(":") ||saveSplitResult[i].contains("Not")|| saveSplitResult[i].equals("")||saveSplitResult[i].contains("Contact Airline") ) {//有空格
							if(saveSplitResult[i].contains(":")) {
								standardEightColumn[j] = saveSplitResult[i];
							}
							i++;
						}else {//长度为1
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
					//System.out.printf("当前填充%d：%s\n",j,Arrays.toString(standardEightColumn));
				}
				//处理完毕一行的信息，添加进Arraylist
				//System.out.printf("%s %s\n",standardEightColumn[0],standardEightColumn[1]);
				flightData.add(standardEightColumn);
			}
			bf.close();
			input.close();
			for(int i=0;i<flightData.size();i++) {
				System.out.printf("第7列情况第%d行：%-25s %-10s\n",i,flightData.get(i)[4],flightData.get(i)[7]);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
