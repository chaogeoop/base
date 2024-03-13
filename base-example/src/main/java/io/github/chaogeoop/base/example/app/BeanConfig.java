package io.github.chaogeoop.base.example.app;

import io.github.chaogeoop.base.business.common.CommonCountProvider;
import io.github.chaogeoop.base.business.elasticsearch.EsProvider;
import io.github.chaogeoop.base.business.mongodb.PersistProvider;
import io.github.chaogeoop.base.business.redis.StrictRedisProvider;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.constants.RabbitmqConstants;
import io.github.chaogeoop.base.example.app.keyregisters.CommonCountKeyRegister;
import io.github.chaogeoop.base.example.app.keyregisters.EsKeyRegister;
import io.github.chaogeoop.base.example.repository.domains.EsSyncLog;
import io.github.chaogeoop.base.example.repository.domains.TestCommonCountDateLog;
import io.github.chaogeoop.base.example.repository.domains.TestCommonCountTotal;
import io.searchbox.client.JestClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Duration;

@Configuration
public class BeanConfig {
    @Autowired
    private JestClient jestClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoTransactionManager mongoTransactionManager;

    @Autowired
    private StrictRedisProvider strictRedisProvider;

    @Autowired
    @Qualifier("taskRabbitTemplate")
    private RabbitTemplate rabbitTemplate;

    @Primary
    @Bean
    public EsProvider esProvider() {
        return new EsProvider(
                this.mongoTemplate,
                EsProvider.RedisAbout.of(
                        this.strictRedisProvider,
                        EsKeyRegister.ES_RESOURCE_FOR_JUDGE_UPDATE_CACHE_TYPE,
                        EsKeyRegister.SYNC_DATA_TO_ES_LOCK_TYPE
                ),
                o -> {
                    this.rabbitTemplate.convertAndSend(RabbitmqConstants.ES_SYNC_QUEUE, JsonHelper.writeValueAsString(o));
                    return null;
                },
                this.jestClient,
                EsSyncLog.class
        );
    }

    @Primary
    @Bean()
    public PersistProvider persistProvider(EsProvider esProvider) {
        return new PersistProvider(this.mongoTemplate, this.mongoTransactionManager, esProvider);
    }

    @Primary
    @Bean()
    public CommonCountProvider commonCountProvider(PersistProvider persistProvider) {
        return new CommonCountProvider(
                persistProvider,
                CommonCountProvider.RedisAbout.of(
                        this.strictRedisProvider,
                        Duration.ofMinutes(1),
                        CommonCountKeyRegister.COMMON_COUNT_TOTAL_CACHE_TYPE,
                        CommonCountKeyRegister.COMMON_COUNT_TOTAL_CREATE_LOCK_TYPE,
                        CommonCountKeyRegister.COUNT_BIZ_DATE_CACHE_TYPE,
                        CommonCountKeyRegister.COMMON_COUNT_PERSIST_HISTORY_PERSIST_LOCK_TYPE,
                        CommonCountKeyRegister.COUNT_BIZ_AFTER_ALL_TOTAL_CACHE_TYPE,
                        CommonCountKeyRegister.getHistoryStoreHashKeyEntity()
                ),
                o -> {
                    this.rabbitTemplate.convertAndSend(RabbitmqConstants.COMMON_COUNT_PERSIST_HISTORY_QUEUE, JsonHelper.writeValueAsString(o));
                    return null;
                },
                TestCommonCountTotal.class,
                TestCommonCountDateLog.class
        );
    }
}
