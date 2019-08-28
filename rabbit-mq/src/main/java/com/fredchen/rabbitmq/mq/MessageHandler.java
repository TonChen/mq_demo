package com.fredchen.rabbitmq.mq;

import org.springframework.amqp.core.Message;

/**
 * @Author: fredChen
 * @Date: 2019/8/28 22:52
 */
public interface MessageHandler {

    void handleMessage(Message message, String name);
}
