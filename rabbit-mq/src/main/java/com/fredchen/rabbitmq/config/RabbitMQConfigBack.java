package com.fredchen.rabbitmq.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 参考：https://blog.csdn.net/charry_a/article/details/80514550
 *
 * @Author: fredChen
 * @Date: 2019/6/11 22:29
 */
@Slf4j
@Configuration
public class RabbitMQConfigBack {

    /**
     * 死信队列 交换机标识符
     */
    private static final String DEAD_LETTER_QUEUE_KEY = "x-dead-letter-exchange";
    /**
     * 死信队列交换机绑定键标识符
     */
    private static final String DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";
    /**
     * 死信队列里面消息的超时时间
     */
    private static final String X_MESSAGE_TTL = "x-message-ttl";


    @Resource
    private RabbitConfig rabbitConfig;


    /**
     * 声明交换机,支持持久化.
     * rabbitmq常用几种exchange,比如direct, fanout, topic,可根据具体业务需求配置
     * 命名规范参考 scm3.services,scm3.services.retry,scm3.services.failed
     *
     * @return the exchange
     */
    @Bean("scm3.materials")
    public Exchange directExchange() {
        return ExchangeBuilder.directExchange("scm3.materials").durable(true).build();
    }

    @Bean("scm3.materials.retry")
    public Exchange retryDirectExchange() {
        return ExchangeBuilder.directExchange("scm3.materials.retry").durable(true).build();
    }

    @Bean("scm3.materials.fail")
    public Exchange failDirectExchange() {
        return ExchangeBuilder.directExchange("scm3.materials.fail").durable(true).build();
    }

    /**
     * 声明一个队列 .{供需关系主队列} 队列名称参考 【服务名称】@订阅服务标识 如
     * material@供需关系,material@供需关系@retry,material@供需关系@failed
     * material@采购计划,material@采购计划@retry,@material@采购计划@failed
     *
     * @return the queue
     */
    @Bean("material@supply")
    public Queue directQueue() {
        return QueueBuilder.durable("material@supply").build();
    }

    /**
     * 供需关系 重试队列
     *
     * @return
     */
    @Bean("material@supply@retry")
    public Queue retryDirectQueue() {
        Map<String, Object> args = new ConcurrentHashMap<>(3);
        // 将消息重新投递到exchange中
        args.put(DEAD_LETTER_QUEUE_KEY, "scm3.materials");
        args.put(DEAD_LETTER_ROUTING_KEY, "material@supply");
        //在队列中延迟30s后，消息重新投递到x-dead-letter-exchage对应的队列中,routingkey是自己配置的
        args.put(X_MESSAGE_TTL, 30 * 1000);
        return QueueBuilder.durable("material@supply@retry").withArguments(args).build();
    }

    /**
     * 供需关系 失败队列
     *
     * @return
     */
    @Bean("material@supply@failed")
    public Queue failDirectQueue() {
        return QueueBuilder.durable("material@supply@failed").build();
    }


    @Bean
    public Binding directBinding(@Qualifier("material@supply") Queue queue,
                                 @Qualifier("scm3.materials") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("direct_rounting_key").noargs();
    }

    @Bean
    public Binding directQueueBinding(@Qualifier("material@supply") Queue queue,
                                      @Qualifier("scm3.materials") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("material@supply").noargs();
    }

    @Bean
    public Binding retryDirectBinding(@Qualifier("material@supply@retry") Queue queue,
                                      @Qualifier("scm3.materials.retry") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("material@supply").noargs();
    }

    @Bean
    public Binding failDirectBinding(@Qualifier("material@supply@failed") Queue queue,
                                     @Qualifier("scm3.materials.fail") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("material@supply").noargs();
    }


    // ===========================================================================

    /**
     * @return the queue
     */
    @Bean("material@user")
    public Queue userDirectQueue() {
        return QueueBuilder.durable("material@user").build();
    }

    /**
     * 用户服务 重试队列
     *
     * @return
     */
    @Bean("material@user@retry")
    public Queue userRetryDirectQueue() {
        Map<String, Object> args = new ConcurrentHashMap<>(3);
        args.put(DEAD_LETTER_QUEUE_KEY, "scm3.materials");
        args.put(DEAD_LETTER_ROUTING_KEY, "material@user");
        args.put(X_MESSAGE_TTL, 30 * 1000);
        return QueueBuilder.durable("material@user@retry").withArguments(args).build();
    }

    /**
     * 供需关系 失败队列
     *
     * @return
     */
    @Bean("material@user@failed")
    public Queue userFailDirectQueue() {
        return QueueBuilder.durable("material@user@failed").build();
    }

    @Bean
    public Binding userDirectBinding(@Qualifier("material@user") Queue queue,
                                     @Qualifier("scm3.materials") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("direct_rounting_key").noargs();
    }

    @Bean
    public Binding userDirectQueueBinding(@Qualifier("material@user") Queue queue,
                                          @Qualifier("scm3.materials") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("material@user").noargs();
    }

    @Bean
    public Binding userRetryDirectBinding(@Qualifier("material@user@retry") Queue queue,
                                          @Qualifier("scm3.materials.retry") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("material@user").noargs();
    }

    @Bean
    public Binding userFailDirectBinding(@Qualifier("material@user@failed") Queue queue,
                                         @Qualifier("scm3.materials.fail") Exchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with("material@user").noargs();
    }


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
        // 生产端不会限流，只有消费端限流；当机器突然有上万条消息，不做限流，可能会导致消费端服务器崩溃。
        // 非自动签收消息的情况下，一定数量消息未被确认前(通过consumer或channel设置qos值)，不进行消费新的消息
        factory.setPrefetchCount(5);
        // 确认模式：自动，默认
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // 接收端类型转化pojo,需要序列化
        factory.setMessageConverter(new Jackson2JsonMessageConverter());
        // 设置消费者线程数
        factory.setConcurrentConsumers(5);
        // 设置最大消费者线程数
        factory.setMaxConcurrentConsumers(10);
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
        // Mandatory：true-监听器接受到这些不可达的消息，false-broker会自动删除这些消息。
        template.setMandatory(true);
        return template;
    }
}
