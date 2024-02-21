package io.github.chaogeoop.base.business.common;

import io.github.chaogeoop.base.business.common.entities.IoStatistic;
import io.github.chaogeoop.base.business.common.interfaces.DefaultResourceInterface;
import io.github.chaogeoop.base.business.common.interfaces.IoMonitorPersist;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.redis.KeyEntity;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.redis.KeyType;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import javax.annotation.Nullable;
import javax.lang.model.type.NullType;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class IoMonitorProvider implements IoMonitorPersist {
    private final RedisAbout<? extends KeyType> redisAbout;
    private final MongoTemplate mongoTemplate;
    private final Function<IoStatistic, NullType> ioMonitorSender;
    private final Class<? extends MonitorLog> logDbClazz;

    public IoMonitorProvider(
            RedisAbout<? extends KeyType> redisAbout,
            MongoTemplate mongoTemplate,
            Function<IoStatistic, NullType> ioMonitorSender,
            Class<? extends MonitorLog> logDbClazz
    ) {
        this.redisAbout = redisAbout;
        this.mongoTemplate = mongoTemplate;
        this.ioMonitorSender = ioMonitorSender;
        this.logDbClazz = logDbClazz;

        BaseModel.getBaseCollectionNameByClazz(this.mongoTemplate, this.logDbClazz);
    }

    @Override
    public void handle(IoStatistic ioStatistic) {
        if (ioStatistic == null) {
            return;
        }

        this.ioMonitorSender.apply(ioStatistic);
    }

    public void persist(IoStatistic newLog, @Nullable Function<MonitorLog, Boolean> existDbLogHandler) {
        DefaultResourceInterface<MonitorLog> entity = new DefaultResourceInterface<>() {

            @Override
            public KeyEntity<? extends KeyType> getLock() {
                return KeyEntity.of(
                        redisAbout.getIoMonitorPersistLockType(),
                        newLog.getFuncName()
                );
            }

            @Override
            public MonitorLog findExist() {
                Query query = new Query();

                query.addCriteria(Criteria.where("funcName").is(newLog.getFuncName()));

                return mongoTemplate.findOne(query, logDbClazz);
            }

            @Override
            public MonitorLog createWhenNotExist() {
                MonitorLog data;
                try {
                    data = logDbClazz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new BizException(String.format("createIoMonitorLog fail: %s", e.getMessage()));
                }

                data.setFuncName(newLog.getFuncName());
                data.setDatabase(newLog.getDatabase());
                data.setCount(newLog.getCount());
                data.setRedis(newLog.getRedis());
                data.setCreated(new Date());

                return data;
            }
        };

        entity.getDefault(this.redisAbout.getRedisProvider(), existDbLog -> {
            if (existDbLog.getId() == null) {
                if (existDbLogHandler != null) {
                    existDbLogHandler.apply(existDbLog);
                }

                existDbLog.calTotal();
                this.mongoTemplate.insert(existDbLog);
                return null;
            }

            boolean needSave = false;
            if (existDbLogHandler != null) {
                needSave = existDbLogHandler.apply(existDbLog);
            } else {
                for (Map.Entry<String, Long> entry : newLog.getDatabase().entrySet()) {
                    Long oldValue = existDbLog.getDatabase().get(entry.getKey());
                    if (oldValue == null || oldValue < entry.getValue()) {
                        existDbLog.getDatabase().put(entry.getKey(), entry.getValue());
                        needSave = true;
                    }
                }

                for (Map.Entry<String, Long> entry : newLog.getCount().entrySet()) {
                    Long oldValue = existDbLog.getCount().get(entry.getKey());
                    if (oldValue == null || oldValue < entry.getValue()) {
                        existDbLog.getCount().put(entry.getKey(), entry.getValue());
                        needSave = true;
                    }
                }

                for (Map.Entry<String, Long> entry : newLog.getRedis().entrySet()) {
                    Long oldValue = existDbLog.getRedis().get(entry.getKey());
                    if (oldValue == null || oldValue < entry.getValue()) {
                        existDbLog.getRedis().put(entry.getKey(), entry.getValue());
                        needSave = true;
                    }
                }
            }


            if (needSave) {
                existDbLog.calTotal();
                this.mongoTemplate.save(existDbLog);
            }

            return null;
        });
    }

    @Setter
    @Getter
    public static class RedisAbout<M extends KeyType> {
        private RedisProvider redisProvider;

        private M ioMonitorPersistLockType;

        public static <M extends KeyType> RedisAbout<M> of(RedisProvider redisProvider, M lockType) {
            RedisAbout<M> data = new RedisAbout<>();

            data.setRedisProvider(redisProvider);
            data.setIoMonitorPersistLockType(lockType);

            return data;
        }
    }

    @Setter
    @Getter
    public static class MonitorLog extends BaseModel {
        @Indexed(unique = true)
        private String funcName;

        private Map<String, Long> database = new HashMap<>();

        private Map<String, Long> count = new HashMap<>();

        private Map<String, Long> redis = new HashMap<>();

        @Indexed
        private Long databaseTotal = 0L;

        @Indexed
        private Long countTotal = 0L;

        @Indexed
        private Long redisTotal = 0L;

        private Date created = new Date();

        public void calTotal() {
            this.databaseTotal = 0L;
            this.countTotal = 0L;
            this.redisTotal = 0L;

            for (Map.Entry<String, Long> entry : this.database.entrySet()) {
                this.databaseTotal += entry.getValue();
            }

            for (Map.Entry<String, Long> entry : this.count.entrySet()) {
                this.countTotal += entry.getValue();
            }

            for (Map.Entry<String, Long> entry : this.redis.entrySet()) {
                this.redisTotal += entry.getValue();
            }
        }
    }
}
