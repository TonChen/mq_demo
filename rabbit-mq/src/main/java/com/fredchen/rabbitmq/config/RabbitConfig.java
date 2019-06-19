package com.fredchen.rabbitmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Author: fredChen
 * @Date: 2019/6/16 14:07
 */

@Data
@Component
@ConfigurationProperties(prefix = "spring.rabbitmq")
public class RabbitConfig {

    private String host;
    private int port;
    private String username;
    private String password;

}
