package com.eqt.needle.notification;

/**
 * little interface to build to if you are going to send messages
 * over Kafka
 */
public interface KMesg<T> {
	public String pack();
	public T unpack(String packed);
}