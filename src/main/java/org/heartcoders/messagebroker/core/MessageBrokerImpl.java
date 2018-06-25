package org.heartcoders.messagebroker.core;

import org.heartcoders.messagebroker.core.exceptions.SubscriberAlreadyExist;
import org.heartcoders.messagebroker.core.exceptions.SubscriberNotExist;
import org.heartcoders.messagebroker.core.storage.MessageStorageFactory;
import org.heartcoders.messagebroker.core.storage.MessageStorageType;
import org.heartcoders.messagebroker.core.storage.MessageStorageProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MessageBrokerImpl implements MessageBroker {
	private ConcurrentHashMap<String,SubscriberInfo> subscribers;
	/**
	 * Topic to Messages
	 */
	private ConcurrentHashMap<String,AtomicLong> topicMessageLogIds;
	/**
	 * Topic to subscriber
	 */
	private ConcurrentHashMap<String,SubscriberInfo> topicToSubscribers;

	private ConcurrentHashMap<String,ConcurrentHashMap<String,Long>> subscriberTopicReadOffsetMap;

	public void subscribe(String id, String topic,SubscriberInfo subscriber) throws SubscriberAlreadyExist {
		if(this.subscribers.contains(id)){
			throw new SubscriberAlreadyExist("The subscriber with id:"+id+"already exist");
		}
		this.subscribers.put(id,subscriber);
		this.topicToSubscribers.put(topic,subscriber);
		ConcurrentHashMap<String, Long> topicToReadOffset = new ConcurrentHashMap<String,Long>();
		topicToReadOffset.put(topic,0l);
		this.subscriberTopicReadOffsetMap.put(id,topicToReadOffset);
	}

	public void unsubscribe(String id, String topic) throws SubscriberNotExist {
		if(this.subscribers.contains(id)){
			if(this.topicToSubscribers.containsKey(topic)){
				this.topicToSubscribers.remove(topic);
			}
		}else{
			throw new SubscriberNotExist(String.format("The subscriber with id:{} doesn't exist.",id));
		}
	}

	public void produce(String topic, Message message) {
		long logId = 0;
		if(!this.topicMessageLogIds.containsKey(topic)){
			this.topicMessageLogIds.put(topic,new AtomicLong(0));
		}else
		{
			AtomicLong id = this.topicMessageLogIds.get(topic);
			logId = id.addAndGet(1);
			this.topicMessageLogIds.put(topic,id);
		}
		this.messageStorageProvider.write(logId,topic,message);
	}

	public Message consume(String subscribeId, String topic) throws SubscriberNotExist {
		Message msg = null;
		if(this.subscribers.containsKey(subscribeId)){
			long offsetId = this.subscriberTopicReadOffsetMap.get(subscribeId).get(topic);
			msg = this.messageStorageProvider.read(topic,offsetId);
			if(msg != null) {
				this.subscriberTopicReadOffsetMap.get(subscribeId).put(topic, ++offsetId);
			}
			return msg;
		}
		throw new SubscriberNotExist("Subscriber must subscribe before consuming.");
	}


	public MessageBrokerImpl(){
		this.messageStorageProvider = MessageStorageFactory.getInstance(MessageStorageType.File);
		this.topicMessageLogIds = new ConcurrentHashMap<String, AtomicLong>();
		this.subscribers = new ConcurrentHashMap<String, SubscriberInfo>();
		this.topicToSubscribers = new ConcurrentHashMap<String, SubscriberInfo>();
		this.subscriberTopicReadOffsetMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();
	}
	MessageStorageProvider messageStorageProvider;

	private static MessageBroker instance;
	private static Object lockObj = new Object();
	public static MessageBroker getInstance(){
		if(instance == null){
			synchronized (lockObj){
				if(instance == null){
					instance = new MessageBrokerImpl();
				}
			}
		}
		return instance;
	}
}
