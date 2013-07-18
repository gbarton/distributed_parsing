package com.gman.util;

import java.util.Map;

public class Constants {

//	public static final String JAR_NAME = "dp.jar";
	public static final String ENV_JAR = "dp_env_jar";
	public static final String ENV_CLASSPATH = "dp_class_path";
	//unique name that we can use for various pathing
	public static final String ENV_NAME = "dp_name";
	public static final String ENV_ZK_URI = "dp_zk_uri";
	public static final String ENV_HDFS_URI = "dp_hdfs_uri";
	
	//discover service constants
	public static final String BROKER_URI = "broker_uri";
	public static final String BROKER_ZK_URI = "broker_zk_uri";
	public static final String TOPIC_DISCOVERY = "disc_topic";
	public static final String TOPIC_CONTROL_UP = "control_up";
	public static final String TOPIC_CONTROL_DOWN = "control_down";
	public static final String TOPIC_WORKUNIT_DOWN = "workunit_down";
	
	
	/**
	 * pulls the constants defined here from IN and sticks em in OUT
	 * probably a better way to do this..
	 */
	public static void fill(Map<String,String> in, Map<String,String> out) {
		insert(in,ENV_JAR,out);
		insert(in,ENV_CLASSPATH,out);
		insert(in,ENV_NAME,out);
		insert(in,ENV_ZK_URI,out);
		insert(in,ENV_HDFS_URI,out);
		insert(in,BROKER_ZK_URI,out);
		
		//also put the classpath in the right var:
		if(in.containsKey(ENV_CLASSPATH))
			out.put("CLASSPATH", in.get(ENV_CLASSPATH));
	}

	private static void insert(Map<String,String> in, String key, Map<String,String> out) {
		if(in.containsKey(key))
			out.put(key,in.get(key));
	}
	
}
