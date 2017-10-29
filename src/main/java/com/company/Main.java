package com.company;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {
	// write your code here

        ZkClient zkClient = null;
        String topicName = "testTopic";
        // zkUtils = null;
        try {
            String zookeeperHosts = "127.0.0.1:2181"; // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
            int sessionTimeOutInMs = 15 * 1000; // 15 secs
            int connectionTimeOutInMs = 10 * 1000; // 10 secs

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = ZkUtils.apply(zkClient, false);


            int noOfPartitions = 2;
            int noOfReplication = 1;
            Properties topicConfiguration = new Properties();

            if(! AdminUtils.topicExists(zkUtils, topicName)) {
                AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration, RackAwareMode.Disabled$.MODULE$);
                System.out.println("");
            }

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
           /* props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);*/
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(props);
            for(int i = 0; i < 1; i++)
                producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));

            producer.close();



        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }


    }
}
