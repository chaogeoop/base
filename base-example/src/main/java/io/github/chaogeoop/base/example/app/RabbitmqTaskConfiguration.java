package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.example.app.constants.RabbitmqConstants;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RabbitmqTaskConfiguration {
    @Value("${rabbitmq.hosts}")
    private String hosts;

    @Value("${rabbitmq.task.username}")
    private String taskUsername;

    @Value("${rabbitmq.task.password}")
    private String taskPasswd;

    @Value("${rabbitmq.task.vhost}")
    private String taskVhost;

    @Bean
    public ConnectionFactory taskConnectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setAddresses(hosts);
        connectionFactory.setUsername(taskUsername);
        connectionFactory.setPassword(taskPasswd);
        connectionFactory.setVirtualHost(taskVhost);
        connectionFactory.setPublisherConfirms(true);
        connectionFactory.setChannelCacheSize(20);
        return connectionFactory;
    }

    @Bean("taskAdmin")
    public RabbitAdmin taskAdmin() {
        return new RabbitAdmin(taskConnectionFactory());
    }

    @Bean("taskRabbitTemplate")
    public RabbitTemplate taskRabbitTemplate() {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(taskConnectionFactory());
        RetryTemplate retryTemplate = new RetryTemplate();
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(10.0);
        backOffPolicy.setMaxInterval(10000);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        rabbitTemplate.setRetryTemplate(retryTemplate);
        return rabbitTemplate;
    }

    @Bean("taskRabbitListener")
    public SimpleRabbitListenerContainerFactory taskRabbitListenerContainerFactory() {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(taskConnectionFactory());
        factory.setMaxConcurrentConsumers(20);
        factory.setChannelTransacted(false);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        return factory;
    }

    @Bean
    public Queue esSyncQueue() {
        Queue queue = new Queue(RabbitmqConstants.ES_SYNC_QUEUE, true, false, false);
        queue.setAdminsThatShouldDeclare(taskAdmin());
        return queue;
    }

    @Bean
    public Queue ioMonitorQueue() {
        Queue queue = new Queue(RabbitmqConstants.IO_MONITOR_QUEUE, true, false, false);
        queue.setAdminsThatShouldDeclare(taskAdmin());
        return queue;
    }

    @Bean
    public Queue commonCountQueue() {
        Queue queue = new Queue(RabbitmqConstants.COMMON_COUNT_PERSIST_HISTORY_QUEUE, true, false, false);
        queue.setAdminsThatShouldDeclare(taskAdmin());
        return queue;
    }
}
