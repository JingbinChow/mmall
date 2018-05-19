package com.mmall.vo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class RocketMQProducer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("pGroup"); //参数为组名
        producer.setNamesrvAddr("192.168.2.217:9876");
        producer.setRetryTimesWhenSendAsyncFailed(5);
//        producer.setDefaultTopicQueueNums(8);
        try {
            producer.start();

            producer.createTopic("TopicTest","TopicTest",8);

            for (int i = 0; i < 10000; i++) {
                try {
                    Message msg = new Message("TopicTest",// topic
                            "TagA",// tag
                            ("Hello RocketMQ " + i).getBytes()// body
                    );
                    SendResult sendResult = producer.send(msg,1000);//该消息1秒没发送成功则重试
//                    System.out.println(sendResult);
                    System.out.println("队列id----------"+sendResult.getMessageQueue().getQueueId());
                }
                catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }

        } catch (MQClientException e) {
            e.printStackTrace();
        }

        producer.shutdown();

    }

}
