package org.heartcoders.messagebroker.core.storage;

import org.heartcoders.messagebroker.core.Message;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

public class MessageFileStorageProvider implements MessageStorageProvider {
	String storeDir = "/Users/satvik/Documents/store";
	ConcurrentHashMap<String,String> topicToFolderPath;

	public MessageFileStorageProvider(){
		this.initialize();
		this.topicToFolderPath = new ConcurrentHashMap<String, String>();
	}
	public void initialize() {
		try {

			File file = new File(storeDir);
			if(!file.exists())
				file.mkdir();
		}
		catch(Exception ex){
			System.out.println("Failed to create storage directory. Storage failed.");
		}
	}

	public void write(long id, String topic, Message msg) {
		String topicFolderPath = "";
		if(this.topicToFolderPath.containsKey(topic)){
			topicFolderPath = this.topicToFolderPath.get(topic);
		}
		else{
			topicFolderPath = this.storeDir+"/"+topic+"/";
			File topicDir = new File(topicFolderPath);
			topicDir.mkdir();
			this.topicToFolderPath.put(topic,topicFolderPath);
		}
		this.writeMessageToFile(topicFolderPath,id,msg);
	}
	private void writeMessageToFile(String path,long id,Message msg){
		String fullPathToMessage = path+id;
		BufferedWriter writer = null;
		try {
			File msgFile = new File(fullPathToMessage);
			msgFile.createNewFile();
			writer = new BufferedWriter(new FileWriter(fullPathToMessage));
			writer.write(msg.getMessage());
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Message read(String topic,long id) {
		String topicFolderPath="";
		if(this.topicToFolderPath.containsKey(topic)){
			topicFolderPath = this.topicToFolderPath.get(topic);
		}
		else{
			return null;
		}
		return this.readMessageInternal(topicFolderPath,id);
	}
	private Message readMessageInternal(String topicPath,long id){
		String fullPathToMessage = topicPath+id;
		BufferedReader reader = null;
		try {
			InputStream is = new FileInputStream(fullPathToMessage);
			BufferedReader buf = new BufferedReader(new InputStreamReader(is));
			String line = buf.readLine();
			StringBuilder sb = new StringBuilder();
			while(line != null)
			{
				sb.append(line).append("\n");
				line = buf.readLine();
			}
			String fileAsString = sb.toString();
			Message msg = new Message(fileAsString);
			buf.close();
			is.close();
			return msg;
		} catch (Exception e) {
			System.out.println("Unable to read message"+e.getMessage());
		}
		return null;
	}
}
