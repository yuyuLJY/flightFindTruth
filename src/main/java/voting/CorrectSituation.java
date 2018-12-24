package voting;

import java.util.HashMap;
import java.util.Map;

public class CorrectSituation {
	public static Map<String,Integer> sourceNumber = new HashMap<String,Integer>();//������ϢԴ������
	public static Map<String,Integer> sourceCorrectNumber = new HashMap<String,Integer>();//������ϢԴ��ȷ������
	public static Map<String,Float> correctRate = new HashMap<String,Float>();//������ϢԴ������
	public static Map<String,String[]> correcInfo = new HashMap<String,String[]>();//��ȷ��Ϣ
	public void setCorrectNumber(String s,int number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!sourceCorrectNumber.containsKey(s)) {
			sourceCorrectNumber.put(s,number);
		}else {
			sourceCorrectNumber.put(s,sourceCorrectNumber.get(s)+number);
		}
    	
    }
    public void setSourceNumber(String s,int number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!sourceNumber.containsKey(s)) {
			sourceNumber.put(s,number);
		}else {
			sourceNumber.put(s,sourceNumber.get(s)+number);
		}
    }
    public void setCorrectRate(String s,float number) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!correctRate.containsKey(s)) {
			correctRate.put(s,number);
		}else {
			System.out.printf("CorrectRate�ظ� %s\n",s);;
		}
    }
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
