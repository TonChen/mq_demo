package com.fredchen.rabbitmq.mq;

import com.fredchen.rabbitmq.config.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: fredChen
 * @Date: 2019/6/11 22:26
 */

@Component
@Slf4j
@RabbitListener(queues = RabbitMQConfig.QUEUE_B)
public class RabbitMQConsumerB1 {

    @RabbitHandler
    public void process(Message message, Channel channel, @Headers Map<String, Object> headers) throws IOException {
        System.err.println("消费端Payload: " + message.getPayload());
        System.err.println("headers: " + headers);
        //requeue 为true会重新入队
        channel.basicAck(message.getHeaders().get(AmqpHeaders.DELIVERY_TAG, long.class), false);
    }

}


