package com.eqt.needle.notification;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.cluster.Broker;

class HostPort {
		public String host;
		public int port;
		
		public HostPort(String hostPort) {
			String[] s = hostPort.split(":");
			host = s[0];
			port = Integer.parseInt(s[1]);
		}
		
		public HostPort(String host, int port) {
			this.host = host;
			this.port = port;
		}
		
		public HostPort(Broker broker) {
			this.host = broker.host();
			this.port = broker.port();
		}
		
		public static Set<HostPort> build(List<String> seeds) {
			Set<HostPort> set = new HashSet<HostPort>();
			for(String seed : seeds) {
				set.add(new HostPort(seed));
			}
			return set;
		}
		
		public String toString() {
			return host + ":" + port;
		}
	}