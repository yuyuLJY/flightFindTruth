package feedbackMethod;

import java.util.ArrayList;

public class Try {
	static double alpha = 0.0001;
	public static void main(String[] args) {
		FCorrectSituation tt = new FCorrectSituation();
		tt.setXRate(0.8947368421052628);
		tt.setYRate(0.12571730271156376);
		tt.setXRate(0.7894736842105261);
		tt.setYRate(0.06713630774436959);
		batchGradientDescent();
	}
	public static double[] batchGradientDescent() {
		double[] theta = {0.1,0.1};
		double[] gradient = {0,0};
		ArrayList<Double> hypothesis = new ArrayList<Double>();
		ArrayList<Double> loss = new ArrayList<Double>();
		//ArrayList<Double> cost = new ArrayList<Double>();
		//ArrayList<Double> gradient = new ArrayList<Double>();
		ArrayList<Double> xRate = new ArrayList<Double>();
		ArrayList<Double> yRate = new ArrayList<Double>();
		FCorrectSituation tt = new FCorrectSituation();
		xRate = tt.getXRate();
		yRate = tt.getYRate();
		double costWhole= 100000;
		for(int iter =0;iter<10000; iter++) {//一直做到降低为止或者到了设定的阈值
			//计算hypothesis
			if(costWhole<0.3) {
				break;
			}
			costWhole = 0;
			double countPerHypothesis;
			for(double a: xRate) {
				countPerHypothesis = a*theta[0]+theta[1];
				hypothesis.add(countPerHypothesis);
			}
			//计算loss
			double countPerLoss;
			for(int i=0;i<yRate.size();i++) {
				countPerLoss = hypothesis.get(i) -yRate.get(i);
				loss.add(countPerLoss);
			}
			//计算cost
			for(Double a :loss) {
				costWhole += a;
			}
			System.out.println("iter:"+iter+" cost:"+costWhole);
			//计算gradient
			double gradient0 = 0;
			double gradient1 = 0;
			for(int i=0;i<yRate.size();i++) {
				gradient0 += xRate.get(i)*loss.get(i); 
				gradient1 += loss.get(i); 
			}
			gradient[0] = gradient0;
			gradient[1] = gradient1;
			//计算theta
			theta[0] = theta[0] - alpha * gradient[0];
			theta[1] = theta[1] - alpha * gradient[1];
			hypothesis.clear();
			loss.clear();
		}
		System.out.println("theta[0]:"+theta[0]+" theta[1]:"+theta[1]);
		return theta;
	}
}
