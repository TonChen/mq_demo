package com.fredchen.rabbitmq.mq;

import com.fredchen.rabbitmq.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @Author: fredChen
 * @Date: 2019/6/11 22:26
 */

@Component
@Slf4j
@RabbitListener(queues = RabbitMQConfig.QUEUE_A)
public class RabbitMQConsumerA {

    private static int count = 0;

    @RabbitHandler
    public void process(String msg, Message message, Channel channel) throws IOException {
        log.info("消费者A消费第{}条消息，消息：{}, deliveryTag：{}", ++count, msg, message.getMessageProperties().getDeliveryTag());
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), true);
    }
}


