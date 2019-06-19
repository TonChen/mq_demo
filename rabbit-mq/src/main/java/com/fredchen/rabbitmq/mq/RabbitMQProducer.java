package com.fredchen.rabbitmq.mq;

import com.fredchen.rabbitmq.config.RabbitMQConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: fredChen
 * @Date: 2019/6/11 22:17
 */

@Slf4j
@Component
public class RabbitMQProducer implements RabbitTemplate.ConfirmCallback {

    /**
     * 由于rabbitTemplate的scope属性设置为ConfigurableBeanFactory.SCOPE_PROTOTYPE，所以不能自动注入
     */
    private RabbitTemplate rabbitTemplate;

    @Autowired
    public RabbitMQProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        // rabbitTemplate如果为单例的话，那回调就是最后设置的内容
        rabbitTemplate.setConfirmCallback(this);
    }

    public void send(String content) {
        CorrelationData correlationId = new CorrelationData(UUID.randomUUID().toString());
        Map<String, Object> map = new HashMap<>();
        map.put("content", content);
        map.put("ext", "ext message");
        rabbitTemplate.convertAndSend(RabbitMQConfig.FANOUT_EXCHANGE, RabbitMQConfig.ROUTING_KEY_B, content, correlationId);
    }

    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        if (ack) {
            log.info("消息ID：{}", correlationData.getId());
            log.info("消息发送成功");
        } else {
            log.info("消息发送失败：", cause);
        }
    }
}
