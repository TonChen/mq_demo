package com.fredchen.rabbitmq.mq;

import com.fredchen.rabbitmq.config.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: fredChen
 * @Date: 2019/6/11 22:17
 */

@Slf4j
@Component
public class RabbitMQProducer implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnCallback {

    @Value("${java.rabbitmq.send.service.exchange}")
    private String sendExchange;
    @Value("${java.rabbitmq.send.service.rountkey}")
    private String rountKey;
    /**
     * 由于rabbitTemplate的scope属性设置为ConfigurableBeanFactory.SCOPE_PROTOTYPE，所以不能自动注入
     */
    private RabbitTemplate rabbitTemplate;

    /**
     * demo级别，先本地缓存,真正实现可考虑用redis 如果是放到redis中，有可能exchange一直不给生产者反馈{比如rabbitmq挂了,这种只能重启rabbitmq}
     * 如果是网络原因，恢复时间应该很快,下次重发的时候网络好了，进行正常的ack 在redis里面，不能设置消息的过期时间,可以用分布式定时任务，每隔一段时间
     * 去查redis里面有没有被消息确认的消息，然后取出来重新发送（存的时候，就要存如当前消息被发送的时间）
     */
    Map<String, org.springframework.amqp.core.Message> messageMap = new ConcurrentHashMap<>();

    @Autowired
    public RabbitMQProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        // rabbitTemplate如果为单例的话，那回调就是最后设置的内容
        rabbitTemplate.setConfirmCallback(this);
        rabbitTemplate.setReturnCallback(this);
    }

    public void send(Object message, Map<String, Object> properties) {
        // 全局唯一，使用时间戳
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        MessageHeaders mhs = new MessageHeaders(properties);
        Message msg = MessageBuilder.createMessage(message, mhs);
        rabbitTemplate.convertAndSend(RabbitMQConfig.FANOUT_EXCHANGE, RabbitMQConfig.ROUTING_KEY_B, msg, correlationId);
    }

    /**
     * 同步发送消息，效率低
     *
     * @param receiveMessage
     */
    public void syncSend(String receiveMessage) {
        org.springframework.amqp.core.Message message = org.springframework.amqp.core.MessageBuilder.withBody(receiveMessage.getBytes())
                .setContentType("application/json").build();
        // 同步等待的超时时间
        rabbitTemplate.setReplyTimeout(3 * 1000);
        Object receiveObject = rabbitTemplate.convertSendAndReceive(sendExchange, rountKey, message);
        System.out.println("生产者收到消费者返回的消息:" + receiveObject);
    }


    /**
     * 异步发送消息， 异步发送，性能更高，但是无法知道消息是否发送到了exchange,可以开启生产端的重试机制
     * spring.rabbitmq.template.retry.enabled=true，默认是false,另外 重试机制默认是重试3次，每次间隔一定时间再次重试,
     *
     * @param receiveMessage
     */
    public void asyncSend(String receiveMessage) {
        String msgId = UUID.randomUUID().toString();
        CorrelationData correlationData = new CorrelationData(msgId);
        // 默认消息就是持久化的 MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;
        org.springframework.amqp.core.Message message = org.springframework.amqp.core.MessageBuilder.withBody(receiveMessage.getBytes())
                .setContentType("application/json").setCorrelationId(msgId).build();
        messageMap.put(msgId, message);
        // 第4个参数是关联发布确定的参数
        try {
            // rabbitTemplate.setMandatory(true);
            // 如果不开启消息回调，可以不要第4个参数，因为在回调时，可以拿到这个correlationData
            // 最后会调用到 void basicPublish(String exchange, String routingKey, boolean mandatory,
            // BasicProperties props, byte[] body)
            // throws IOException;
            rabbitTemplate.convertAndSend(sendExchange, rountKey, message, correlationData);
            log.info("生产者发送消息:" + receiveMessage + ",消息Id：" + msgId);
        } catch (AmqpException e) {
            log.info("生产者发送消息:" + receiveMessage + "发生了异常:" + e.getMessage());
        }
    }


    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        if (ack) {
            log.info("消息ID：{}", correlationData.getId());
            log.info("消息发送成功");
        } else {
            log.info("消息发送失败：{}", cause);
        }

        if (null != messageMap && !messageMap.isEmpty()) {
            if (!"".equals(cause)) {
                System.out.println("失败原因:" + cause);
                // 重发的时候到redis里面取,消费成功了，删除redis里面的msgId
                org.springframework.amqp.core.Message message = messageMap.get(correlationData.getId());
                rabbitTemplate.convertAndSend(sendExchange, rountKey, message, correlationData);
            } else {
                messageMap.remove(correlationData.getId());
                System.out.println("消息唯一标识:" + correlationData + ";确认结果:" + ack);
            }
        }
    }

    /**
     * 如果发送的消息，Exchange不存在或者RouteKey路由不到，这时就需要returnListener。
     *
     * @param message
     * @param replyCode
     * @param replyText
     * @param exchange
     * @param routingKey
     */
    @Override
    public void returnedMessage(org.springframework.amqp.core.Message message, int replyCode, String replyText, String exchange, String routingKey) {
        log.info("消息发送失败：{}，{}，{}，{}，{}", message.toString(), replyCode, replyText, exchange, routingKey);
    }

}
