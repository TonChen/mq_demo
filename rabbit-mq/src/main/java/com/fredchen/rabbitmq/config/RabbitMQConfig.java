package com.fredchen.rabbitmq.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.Resource;

/**
 * @Author: fredChen
 * @Date: 2019/6/11 22:29
 */

@Slf4j
@Configuration
public class RabbitMQConfig {

    public static final String DEFAULT_EXCHANGE = "default_exchange";
    public static final String FANOUT_EXCHANGE = "fanout_exchange";
    public static final String TOPIC_EXCHANGE = "topic_exchange";

    public static final String ROUTING_KEY_A = "routing_key_A";
    public static final String ROUTING_KEY_B = "routing_key_B";

    public static final String QUEUE_A = "queue_A";
    public static final String QUEUE_B = "queue_B";

    @Resource
    private RabbitConfig rabbitConfig;


    /**
     * 针对消费者配置
     * 1. 设置交换机类型
     * 2. 将队列绑定到交换机
     * FanoutExchange: 将消息分发到所有的绑定队列，无routingkey的概念
     * HeadersExchange ：通过添加属性key-value匹配
     * DirectExchange:按照routingkey分发到指定队列
     * TopicExchange:多关键字匹配
     */
    @Bean
    public DirectExchange defaultExchange() {
        return new DirectExchange(DEFAULT_EXCHANGE);
    }

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(FANOUT_EXCHANGE);
    }

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(TOPIC_EXCHANGE);
    }

    /**
     * 声明一个队列A
     *
     * @return
     */
    @Bean
    public Queue queueA() {
        return new Queue(QUEUE_A, false, false, true);
    }

    /**
     * 声明一个队列B
     *
     * @return
     */
    @Bean
    public Queue queueB() {
        return new Queue(QUEUE_B, false, false, true);
    }

    //====================================同一个交换机exchange，绑定不同的队列，绑定同一个key====================================

    /**
     * 使队列和交换机绑定，同时指定routing_key
     * 一个交换机可以绑定多个队列，即通过一个交换机可以把消息发送到多个队列
     *
     * @return
     */
    @Bean
    public Binding bindingQueueAWithKeyA() {
        return BindingBuilder.bind(queueA()).to(defaultExchange()).with(ROUTING_KEY_A);
    }

    @Bean
    public Binding bindingQueueBWithKeyA() {
        return BindingBuilder.bind(queueB()).to(defaultExchange()).with(ROUTING_KEY_A);
    }

    //=================================================================================

    @Bean
    public Binding bindingQueueC() {
        return BindingBuilder.bind(queueB()).to(fanoutExchange());
    }

    @Bean
    public Binding bindingQueueB() {
        return BindingBuilder.bind(queueB()).to(fanoutExchange());
    }

    @Bean
    public Binding bindingQueueA() {
        return BindingBuilder.bind(queueA()).to(fanoutExchange());
    }
    //=================================================================================
    //=================================================================================

    @Bean
    public Binding bindingQueueBWithA() {
        return BindingBuilder.bind(queueB()).to(topicExchange()).with(ROUTING_KEY_A);
    }

    @Bean
    public Binding bindingQueueBWithB() {
        return BindingBuilder.bind(queueB()).to(topicExchange()).with(ROUTING_KEY_B);
    }

    @Bean
    public Binding bindingQueueAWithB() {
        return BindingBuilder.bind(queueA()).to(topicExchange()).with(ROUTING_KEY_B);
    }
    //=================================================================================


    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setUsername(rabbitConfig.getUsername());
        factory.setPassword(rabbitConfig.getPassword());
        factory.setVirtualHost("/");
        factory.setHost(rabbitConfig.getHost());
        factory.setPort(rabbitConfig.getPort());
        // 保证消息的事务性处理rabbitmq默认的处理方式为auto, true表示发送端需要确认消息发送
        factory.setPublisherConfirms(true);
        factory.setPublisherReturns(true);
        // ack，这意味着当你从消息队列取出一个消息时，ack自动发送，mq就会将消息删除。而为了保证消息的正确处理，我们需要将消息处理修改为手动确认的方式
        return factory;
    }


    /**
     * 配置接收端属性
     *
     * @param connectionFactory
     * @return
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        //这个参数设置，接收消息端，接收的最大消息数量（包括使用get、consume）,一旦到达这个数量，客户端不在接收消息。0为不限制。默认值为3.
        //factory.setPrefetchCount(5);
        // 确认模式：自动，默认
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // 接收端类型转化pojo,需要序列化
        factory.setMessageConverter(new Jackson2JsonMessageConverter());
        // 设置消费者线程数
        factory.setConcurrentConsumers(5);
        // 设置最大消费者线程数
        //factory.setMaxConcurrentConsumers(10);
//        factory.setConsumerTagStrategy(s -> {
//            log.info("消费确认：{}", s);
//            return "RGP订单系统ADD处理逻辑消费者";
//        });
        return factory;
    }

    /**
     * 必须是prototype类型,不然每次回调都是最后一个内容
     *
     * @param connectionFactory
     * @return
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {

        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        // 发送端类型转化pojo,需要序列化
        template.setMessageConverter(new Jackson2JsonMessageConverter());
        return template;
    }
}
