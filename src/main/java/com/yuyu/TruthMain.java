package com.yuyu;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

public class TruthMain {
	public static final String HDFS = "hdfs://192.168.126.130:9000";
	public static void main(String[] args) throws Exception {
		Map<String,String> path = new HashMap<String,String>();
		path.put("Step1Input", HDFS+"/user/findTruth/data/clean_flight/2011-12-01-data.txt");
		path.put("Step1Output", HDFS+"/user/findTruth/data/standard_clean_flight");
		Step1.run(path);//产生标准数据集
	}
	
    public static JobConf config() {
        JobConf conf = new JobConf(TruthMain.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}
