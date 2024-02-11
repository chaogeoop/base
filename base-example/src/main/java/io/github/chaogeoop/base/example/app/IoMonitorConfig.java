package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.business.common.IoMonitorProvider;
import io.github.chaogeoop.base.business.common.annotations.EnableMongoIoMonitor;
import io.github.chaogeoop.base.business.common.annotations.EnableRedisIoMonitor;
import io.github.chaogeoop.base.business.common.annotations.IoMonitorAspect;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.constants.RabbitmqConstants;
import io.github.chaogeoop.base.example.app.keyregisters.IoKeyRegister;
import io.github.chaogeoop.base.example.repository.domains.IoMonitorLog;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

@EnableMongoIoMonitor
@EnableRedisIoMonitor
@Configuration
public class IoMonitorConfig {
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RedisProvider redisProvider;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Bean
    public IoMonitorProvider ioMonitorProvider() {
        return new IoMonitorProvider(
                IoMonitorProvider.RedisAbout.of(
                        this.redisProvider,
                        IoKeyRegister.IO_MONITOR_LOG_PERSIST_LOCK_TYPE
                ),
                this.mongoTemplate,
                o -> {
                    this.rabbitTemplate.convertAndSend(RabbitmqConstants.IO_MONITOR_QUEUE, JsonHelper.writeValueAsString(o));
                    return null;
                },
                IoMonitorLog.class
        );
    }

    //如果一个项目引进了多个数据库,ioMonitorLog只会被持久化到注入的mongoTemplate对应的数据库
    @Bean
    public IoMonitorAspect ioMonitorAspect(IoMonitorProvider ioMonitorProvider) {
        return new IoMonitorAspect(ioMonitorProvider, true);
    }
}
