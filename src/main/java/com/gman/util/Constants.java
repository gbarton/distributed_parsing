package com.gman.util;

import java.util.Map;

public class Constants {

	public static final String ENV_APPID = "needle_app_id";
	public static final String ENV_JAR = "needle_env_jar";
	public static final String ENV_JAR_PATH = "needle_env_jar_path";
	public static final String ENV_CLASSPATH = "needle_class_path";

	//unique name that we can use for various pathing
	public static final String ENV_NAME = "needle_app_name";
	public static final String ZK_URI_BASE_PATH = "needle";
	//path is used as a way to store key/value props for the client to use. such as broker location
	public static final String ZK_CLIENT_BASE_PATH = "/clientProps";
	public static final String CLIENT_BROKER_LOCATION = "broker";
	
	public static final String ENV_ZK_URI = "needle_zk_uri";
	public static final String ENV_HDFS_URI = "needle_hdfs_uri";
	
	//discover service constants
	public static final String BROKER_URI = "needle_broker_uri";
	public static final String BROKER_ZK_URI = "needle_broker_zk_uri";
	
	//Topics for communications
	public static final String TOPIC_CLIENT_FEED = "needle_client_topic";
	public static final String TOPIC_DISCOVERY = "needle_disc_topic";
	public static final String TOPIC_CONTROL_UP = "control_up";
	public static final String TOPIC_CONTROL_DOWN = "control_down";
	public static final String TOPIC_WORKUNIT_DOWN = "workunit_down";
	
	
	/**
	 * pulls the constants defined here from IN and sticks em in OUT
	 * probably a better way to do this..
	 */
	public static void fill(Map<String,String> in, Map<String,String> out) {
		insert(in,ENV_APPID,out);
		insert(in,ENV_JAR,out);
		insert(in,ENV_JAR_PATH,out);
		insert(in,ENV_CLASSPATH,out);
		insert(in,ENV_NAME,out);
		insert(in,ENV_ZK_URI,out);
		insert(in,ENV_HDFS_URI,out);
		insert(in,BROKER_URI,out);
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
