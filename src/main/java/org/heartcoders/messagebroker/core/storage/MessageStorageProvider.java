package org.heartcoders.messagebroker.core.storage;

import org.heartcoders.messagebroker.core.Message;

public interface MessageStorageProvider {
	void initialize();
	void write(long id,String topic,Message msg);
	Message read(String topic,long id);
}
