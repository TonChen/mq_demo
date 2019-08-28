package com.fredchen.rabbitmq.web;

import com.fredchen.rabbitmq.mq.RabbitMQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * @Author: fredChen
 * @Date: 2019/6/17 22:32
 */

@RestController
public class MessgeController {

    @Autowired
    private RabbitMQProducer producer;

    @RequestMapping("/testA")
    public Object testMessageA() {
        for (int i = 0; i < 3; i++) {
            producer.send("testA发送消息" + i, new HashMap<>());
        }
        return "OK";
    }

}
