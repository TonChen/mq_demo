package com.fredchen.rabbitmq;

import com.fredchen.rabbitmq.mq.RabbitMQProducer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class RabbitMqApplicationTests {

    @Autowired
    private RabbitMQProducer rabbitMQProducer;

    @Test
    public void producerTest() {
        rabbitMQProducer.send("我的测试");
        while (true) {
            log.info("system is running");
        }
    }

}
