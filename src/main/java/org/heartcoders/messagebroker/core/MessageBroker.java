package org.heartcoders.messagebroker.core;

import org.heartcoders.messagebroker.core.exceptions.SubscriberAlreadyExist;
import org.heartcoders.messagebroker.core.exceptions.SubscriberNotExist;

public interface MessageBroker {
	void subscribe(String id,String topic,SubscriberInfo subscriber) throws SubscriberAlreadyExist;
	void unsubscribe(String id,String topic) throws SubscriberNotExist;
	void produce(String topic,Message message);
	Message consume(String subscribeId,String topic) throws SubscriberNotExist;
}
