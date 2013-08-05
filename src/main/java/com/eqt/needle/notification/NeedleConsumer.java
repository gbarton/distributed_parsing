package com.eqt.needle.notification;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Totally based on the example code found here:
 * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 * 
 * This class handles a single partition of a single topic.
 * the class will come online and try to connect to a given topics partition, it expects
 * the partition and topic to be available to talk to.
 *  
 * TODO: need wrapper class to handle all partitions of a topic.
 */
public class NeedleConsumer implements Runnable {
	private static final Log LOG = LogFactory.getLog(NeedleConsumer.class);
	private Set<HostPort> replicaBrokers = new HashSet<HostPort>();
	private Set<HostPort> seedBrokers;
	private String topic;
	private int partition;
	private HostPort leadBroker;
	private boolean shutdown = false;
	//initialized to a somewhat small capacity to limit ram usage
	//TODO: make configurable??
	private ArrayBlockingQueue<Message<String,String>> messages = new ArrayBlockingQueue<Message<String,String>>(10);
	
	//client info
	private String clientName;
	private static final int timeOut = 100000;
	private static final int bufferSize = 64*1024;

	/**
	 * @param topic topic to talk with.
	 * @param partition which partition to connect to.
	 * @param seedBrokers expected host:port strings.
	 * 
	 */
	public NeedleConsumer(String topic, int partition, List<String> seedBrokers) {
		this.topic = topic;
		this.partition = partition;
		if(seedBrokers != null) {
			this.seedBrokers = new HashSet<HostPort>();
			for(String seed : seedBrokers) {
				this.seedBrokers.add(new HostPort(seed));
			}
		} else {
			throw new IllegalArgumentException("initial Seed list of brokers null");
		}
		if(this.seedBrokers.size() < 1)
			throw new IllegalArgumentException("could not get a single host:port combo from seeds.");
		
		// find the meta data about the topic and partition we are interested in
		PartitionMetadata metadata = findLeader(this.seedBrokers, topic, partition);
		if (metadata == null) {
			LOG.error("Can't find metadata for Topic and Partition. Exiting");
			throw new IllegalStateException("Can't find metadata for Topic and Partition.");
		}
		if (metadata.leader() == null) {
			LOG.error("Can't find Leader for Topic and Partition. Exiting");
			throw new IllegalStateException("Can't find Leader for Topic and Partition.");
		}
		leadBroker = new HostPort(metadata.leader());
		clientName = "Client_" + topic + "_" + partition;

	}

	public Message<String,String> getNextMessage() {
		LOG.debug("getNextMessage Called for topic: " + topic + " partition: " + partition);
		return messages.poll();
	}
	
	public void close() {
		this.shutdown = true;
	}
	
	private SimpleConsumer getConsumer(HostPort broker) {
		return getConsumer(broker, clientName);
	}
	
	private SimpleConsumer getConsumer(HostPort broker, String name) {
		return new SimpleConsumer(broker.host, broker.port, timeOut, bufferSize, clientName);
	}
	
	/**
	 * Spins indefinitely until the close method is called.
	 */
	public void run() {
		LOG.info("consumer thread started for topic: " + topic + " partition: " + partition);
		SimpleConsumer consumer = getConsumer(leadBroker);
		long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(),
				clientName);

		int numErrors = 0;
		while (!shutdown) {
			if (consumer == null) {
				consumer = getConsumer(leadBroker);
			}
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(topic, partition, readOffset, timeOut).build();
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(topic, partition);
				LOG.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (numErrors > 5) //TODO: do i want this?? maybe throw up
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for
					// the last element to reset
					readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(),
							clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, topic, partition);
				continue;
			}
			numErrors = 0;

			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic,partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					LOG.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				try {
					LOG.debug("##################### received a message! on topic: " + topic + " partition " + partition);
					messages.put(new Message<String, String>(
							convertBytesToString(messageAndOffset.message().key()),
							convertBytesToString(messageAndOffset.message().payload())));
				} catch (UnsupportedEncodingException e) {
					LOG.error("message failure to decode bytes into UTF-8 format.",e);
					throw new RuntimeException("Cannot decode the messages into UTF-8 format",e);
				} catch (InterruptedException e) {
					LOG.error("interupted waiting to add to the queue",e);
					throw new RuntimeException("Cannot add to my own queue");
				}
				numRead++;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		if (consumer != null)
			consumer.close();
		LOG.info("shutdown complete");
	}

	private String convertBytesToString(ByteBuffer buff) throws UnsupportedEncodingException {
		byte[] bytes = new byte[buff.limit()];
		buff.get(bytes);
		return new String(bytes, "UTF-8");
	}
	
	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
			String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: "
					+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private HostPort findNewLeader(HostPort oldLeader, String a_topic, int a_partition) {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(replicaBrokers, a_topic, a_partition);
			if (metadata == null)
				goToSleep = true;
			else if (metadata.leader() == null)
				goToSleep = true;
			else if (oldLeader.host.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				goToSleep = true;
			} else
				return new HostPort(metadata.leader());
			if (goToSleep)
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
					LOG.warn("having troubles sleeping",ie);
				}
		}
		LOG.error("Unable to find new leader after Broker failure:" + oldLeader);
		throw new IllegalStateException("Unable to find new leader after Broker failure: " + oldLeader);
	}

	private PartitionMetadata findLeader(Set<HostPort> brokers, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		for (HostPort hp : brokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(hp.host, hp.port, 100000, 64 * 1024, "leaderLookup");
				List<String> topics = new ArrayList<String>();
				topics.add(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData)
					for (PartitionMetadata part : item.partitionsMetadata())
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break;
						}
			} catch (Exception e) {
				LOG.error("Error communicating with Broker [" + hp + "] to find Leader for [" + a_topic
						+ ", " + a_partition + "] Reason: ", e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas())
				replicaBrokers.add(new HostPort(replica.host(),replica.port()));
		}
		return returnMetaData;
	}
}
