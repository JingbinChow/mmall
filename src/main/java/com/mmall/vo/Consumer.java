package com.mmall.vo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Consumer {


    public static void main(String[] args)  throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cGroup");
        consumer.setNamesrvAddr("192.168.2.217:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        /**
         * 主题Tpoic：第一级消息类型，书的标题
         * 标签Tags：第二级消息类型，书的目录，可以基于Tag做消息过滤
         */
        consumer.subscribe("TopicTest", //指定消费主题是topicTest
                "*");   //tag为TagA tagB的消息


        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                /**
                 * List<MessageExt> msgs  这里虽然是list 但实际上基本都只会是一条，除非消息堆积，
                 * 且记住一定是先启动消费者,再启动生产者
                 * 否则极有可能导致消息的重复消费
                 *
                 */
                for (int i=0 ; i<msgs.size();i++){
                    try {
                        System.out.println("消费了一条消息："+new String(msgs.get(i).getBody(),"utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        //消费失败告诉mq重新发送继续消费  如果多次消费仍不成功可以记录在数据库中，可以通过mext.getReconsumeTimes()获取消费次数
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }

//                for(MessageExt mext : msgs) {
//                    try {
//                        System.out.println("消费了一条消息："+new String(mext.getBody(),"utf-8"));
//                    } catch (UnsupportedEncodingException e) {
//                        // TODO Auto-generated catch block
//                        e.printStackTrace();
//                        //消费失败告诉mq重新发送继续消费  如果多次消费仍不成功可以记录在数据库中，可以通过mext.getReconsumeTimes()获取消费次数
//                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                    }
//                }
                /*
                 * 告诉mq消费成功
                 */
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;


            }
        });

        consumer.start();

        System.out.println("Consumer Started.");

    }
}
