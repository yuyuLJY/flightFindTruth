package voting;

import java.util.HashMap;
import java.util.Map;

public class CorrectSituation {
	public static Map<String,Integer> sourceNumber = new HashMap<String,Integer>();//计算信息源的数量
	public static Map<String,Integer> sourceCorrectNumber = new HashMap<String,Integer>();//计算信息源正确的数量
	public static Map<String,Float> correctRate = new HashMap<String,Float>();//计算信息源的数量
	public static Map<String,String[]> correcInfo = new HashMap<String,String[]>();//正确信息
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
			correctRate.put(s,correctRate.get(s)+number);
		}
    }
    public void setCorrectInfo(String s,String[] info) {
    	//System.out.printf("main %s %d\n",s,number);
		if(!correcInfo.containsKey(s)) {
			correcInfo.put(s,info);
		}else {
			System.out.println("correctInfo重复");
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
