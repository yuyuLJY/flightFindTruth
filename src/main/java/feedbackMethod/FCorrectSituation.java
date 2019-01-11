package feedbackMethod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FCorrectSituation {
	public static Map<String,Integer> sourceNumber = new HashMap<String,Integer>();//计算信息源的数量
	public static Map<String,Integer> sourceCorrectNumber = new HashMap<String,Integer>();//计算信息源正确的数量
	public static Map<String,Float> correctRate = new HashMap<String,Float>();//计算信息源的数量
	public static Map<String,String[]> correcInfo = new HashMap<String,String[]>();//正确信息
	
	public static Map<String,Double> RealityI = new HashMap<String,Double>();//存储本次i正确率
	public static Map<String,Double> Dcate = new HashMap<String,Double>();//
	public static Map<String,Double> RealityJ = new HashMap<String,Double>();//存储本次i正确率
	public static Map<String,Double> Dcon = new HashMap<String,Double>();//
	public static ArrayList<Double> xRate = new ArrayList<Double>();
	public static ArrayList<Double> yRate = new ArrayList<Double>();
	public static ArrayList<Double> beta = new ArrayList<Double>();
	public static ArrayList<Double> alpha = new ArrayList<Double>();
	public static int Iter=0;
	public static double correctNumber=0;
	public static double wrongNumber=0;
	public static Map<String,Double> perSourceCorrectRate = new HashMap<String,Double>();//
	//计算梯度下降法迭代的次数
	public void setIter(int iter) {
		Iter = iter;
	}
	public int getIter() {
		return Iter;
	}
	
	//设置本次的正确率存储 ：<"boston",0.2>
	public void setperSourceCorrectRate(String s,double rate){
		perSourceCorrectRate.put(s, rate);
	}
	public Map<String,Double> getperSourceCorrectRate(){
		return perSourceCorrectRate;
	}
	
	//在验证阶段，每个信息源正确的情况
	public void setCorrect(double number) {
		correctNumber = number;
	}
	public double getCorrect() {
		return correctNumber;
	}
	
	public void setWrong(double number) {
		wrongNumber = number;
	}
	public double getWrong() {
		return wrongNumber;
	}
	public void setBeta(double rate) {
		beta.add(rate);
	}
	
	public double getLastBeta() {
		return beta.get(beta.size()-1);
	}
	
	public void setAlpha(double rate) {
		alpha.add(rate);
	}
	
	public double getLastAlpha() {
		return alpha.get(alpha.size()-1);
	}
	
	//设置本次的正确率存储 ：<"boston",0.2>
	public void setRealityI(String s,double rate){
		RealityI.put(s, rate);
	}
	public Map<String,Double> getRealityI(){
		return RealityI;
	}
	
	//设置本次的正确率存储 ：<"boston",0.2>
	public void setRealityJ(String s,double rate){
		RealityJ.put(s, rate);
	}
	
	public Map<String,Double> getRealityJ(){
		return RealityJ;
	}
	
	//文本型判断结果
	public void setDcate(String s,Double rate){
			Dcate.put(s, rate);
	}
	
	public Map<String,Double> getDcate(){
		return Dcate;
	}
	
	public void clearDcate(){
		this.getDcate().clear();
	}
	
	//数值型判断结果
	public void setDcon(String s,Double rate){
			Dcon.put(s, rate);
	}
	
	public Map<String,Double> getDcon(){
		return Dcon;
	}
	
	//新的一轮计算要清空
	public void clearDcon(){
		this.getDcon().clear();
	}
	//
	public void setXRate(double rate){
		xRate.add(rate);
	}
	
	public void setYRate(double rate){
		yRate.add(rate);
	}
	
	public ArrayList<Double> getXRate() {
		return xRate;
	}
	
	public ArrayList<Double> getYRate() {
		return yRate;
	}
	
	//正确的数量
	public void setCorrectNumber(String s,int number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!sourceCorrectNumber.containsKey(s)) {
			sourceCorrectNumber.put(s,number);
		}else {
			sourceCorrectNumber.put(s,sourceCorrectNumber.get(s)+number);
		}
    	
    }
	
	//信息源的数量
    public void setSourceNumber(String s,int number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!sourceNumber.containsKey(s)) {
			sourceNumber.put(s,number);
		}else {
			sourceNumber.put(s,sourceNumber.get(s)+number);
		}
    }
    
    //设置正确率
    public void setCorrectRate(String s,float number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!correctRate.containsKey(s)) {
			correctRate.put(s,number);
		}else {
			System.out.printf("CorrectRate重复 %s\n",s);;
		}
    }
    
    //设置正确信息 <航班信息,正确的信息>
    public void setCorrectInfo(String s,String[] info) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!correcInfo.containsKey(s)) {
			correcInfo.put(s,info);
		}else {
			correcInfo.put(s,info);
			System.out.printf("correctInfo重复 %s\n",s);
		}
    }
    
    public  Map<String,Integer> getCorrectNumber() {
    	//System.out.printf("main %s %d\n",s,number);
    	return sourceCorrectNumber;
    }
    public  Map<String,Integer> getSourceNumber() {
    	//System.out.printf("main %s %d\n",s,number);
    	return sourceNumber;
    }
    public  Map<String,Float> getCorrectRate() {
    	//System.out.printf("main %s %d\n",s,number);
    	return correctRate;
    }
    public  Map<String,String[]> getCorrecInfo() {
    	//System.out.printf("main %s %d\n",s,number);
    	return correcInfo;
    }
    
}
