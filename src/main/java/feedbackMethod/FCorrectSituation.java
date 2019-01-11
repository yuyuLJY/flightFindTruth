package feedbackMethod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FCorrectSituation {
	public static Map<String,Integer> sourceNumber = new HashMap<String,Integer>();//������ϢԴ������
	public static Map<String,Integer> sourceCorrectNumber = new HashMap<String,Integer>();//������ϢԴ��ȷ������
	public static Map<String,Float> correctRate = new HashMap<String,Float>();//������ϢԴ������
	public static Map<String,String[]> correcInfo = new HashMap<String,String[]>();//��ȷ��Ϣ
	
	public static Map<String,Double> RealityI = new HashMap<String,Double>();//�洢����i��ȷ��
	public static Map<String,Double> Dcate = new HashMap<String,Double>();//
	public static Map<String,Double> RealityJ = new HashMap<String,Double>();//�洢����i��ȷ��
	public static Map<String,Double> Dcon = new HashMap<String,Double>();//
	public static ArrayList<Double> xRate = new ArrayList<Double>();
	public static ArrayList<Double> yRate = new ArrayList<Double>();
	public static ArrayList<Double> beta = new ArrayList<Double>();
	public static ArrayList<Double> alpha = new ArrayList<Double>();
	public static int Iter=0;
	
	//�����ݶ��½��������Ĵ���
	public void setIter(int iter) {
		Iter = iter;
	}
	public int getIter() {
		return Iter;
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
	
	//���ñ��ε���ȷ�ʴ洢 ��<"boston",0.2>
	public void setRealityI(String s,double rate){
		RealityI.put(s, rate);
	}
	public Map<String,Double> getRealityI(){
		return RealityI;
	}
	
	//���ñ��ε���ȷ�ʴ洢 ��<"boston",0.2>
	public void setRealityJ(String s,double rate){
		RealityJ.put(s, rate);
	}
	
	public Map<String,Double> getRealityJ(){
		return RealityJ;
	}
	
	//�ı����жϽ��
	public void setDcate(String s,Double rate){
			Dcate.put(s, rate);
	}
	
	public Map<String,Double> getDcate(){
		return Dcate;
	}
	
	public void clearDcate(){
		this.getDcate().clear();
	}
	
	//��ֵ���жϽ��
	public void setDcon(String s,Double rate){
			Dcon.put(s, rate);
	}
	
	public Map<String,Double> getDcon(){
		return Dcon;
	}
	
	//�µ�һ�ּ���Ҫ���
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
	
	//��ȷ������
	public void setCorrectNumber(String s,int number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!sourceCorrectNumber.containsKey(s)) {
			sourceCorrectNumber.put(s,number);
		}else {
			sourceCorrectNumber.put(s,sourceCorrectNumber.get(s)+number);
		}
    	
    }
	
	//��ϢԴ������
    public void setSourceNumber(String s,int number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!sourceNumber.containsKey(s)) {
			sourceNumber.put(s,number);
		}else {
			sourceNumber.put(s,sourceNumber.get(s)+number);
		}
    }
    
    //������ȷ��
    public void setCorrectRate(String s,float number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!correctRate.containsKey(s)) {
			correctRate.put(s,number);
		}else {
			System.out.printf("CorrectRate�ظ� %s\n",s);;
		}
    }
    
    //������ȷ��Ϣ <������Ϣ,��ȷ����Ϣ>
    public void setCorrectInfo(String s,String[] info) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!correcInfo.containsKey(s)) {
			correcInfo.put(s,info);
		}else {
			correcInfo.put(s,info);
			System.out.printf("correctInfo�ظ� %s\n",s);
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
