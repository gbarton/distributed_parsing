package com.gman.util;

public class IOUtils {

	public static String makeAbsolutePath(String ... args) {
		String str = "";
		for(String s : args) {
			if(!str.endsWith("/"))
				str+="/";
			str+=s;
		}
		return str;
	}
	
}
