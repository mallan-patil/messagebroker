package org.heartcoders.messagebroker.core.storage;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MessageStorageFactory {
	public static MessageStorageProvider getInstance(MessageStorageType messageStorageType){
		if(messageStorageType == MessageStorageType.File){
			return new MessageFileStorageProvider();
		}
		throw new NotImplementedException();
	}
}
