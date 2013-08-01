package com.eqt.needle.sample;

/**
 * little wrapper class to serialize out over kafka.
 */
public class Payload {
	//what the current cpu usage was on this host.
	public double loadAverage;
	public int cores;
	//when did it occur.
	public long happenedAt;
}
