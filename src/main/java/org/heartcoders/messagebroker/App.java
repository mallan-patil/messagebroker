package org.heartcoders.messagebroker;

import org.heartcoders.messagebroker.core.Message;
import org.heartcoders.messagebroker.core.MessageBroker;
import org.heartcoders.messagebroker.core.MessageBrokerImpl;
import org.heartcoders.messagebroker.core.SubscriberInfo;
import org.heartcoders.messagebroker.core.SubscriberInfo;
import org.heartcoders.messagebroker.core.exceptions.SubscriberAlreadyExist;
import org.heartcoders.messagebroker.core.exceptions.SubscriberNotExist;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        MessageBroker messageBroker = MessageBrokerImpl.getInstance();

        System.out.println("\n\n-- Producing...");
        produceMesage(messageBroker, "flip", "My First message");
        produceMesage(messageBroker, "flip", "My second message");
        produceMesage(messageBroker, "flip", "My third message");

        SubscribeAndConsume(messageBroker,"sub1","flip");
        SubscribeAndConsume(messageBroker,"sub2","flip");

    }

    private static void SubscribeAndConsume(MessageBroker messageBroker, String subId, String topic) {
        System.out.println("\nSubscriber:"+subId+" is consuming..\n");
        SubscriberInfo subInfo = new SubscriberInfo();
        try {
            messageBroker.subscribe(subId, topic, subInfo);

        } catch (SubscriberAlreadyExist subscriberAlreadyExist) {
            subscriberAlreadyExist.printStackTrace();
        }
        Message msg = null;
        do {
            try {

                msg = messageBroker.consume(subId, topic);
            } catch (SubscriberNotExist subscriberNotExist) {
                subscriberNotExist.printStackTrace();
            }
            if (msg != null) {
                System.out.println("Message read is:" + msg.getMessage());
            }
        }while(msg != null);

    }


    public static void produceMesage(MessageBroker messageBroker, String topic,String message){
        System.out.println("Producing message:"+message);
        Message msg1 = new Message(message);
        messageBroker.produce(topic,msg1);
    }

    public static void concurrentProducing(){
        int noOfThreads = 5;
        List<Thread> threads = new ArrayList<Thread>(noOfThreads);

    }
}

