package io.github.chaogeoop.base.business.common;

import io.github.chaogeoop.base.business.common.entities.IoStatistic;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.common.interfaces.DefaultResourceInterface;
import io.github.chaogeoop.base.business.common.interfaces.IoMonitorPersist;
import io.github.chaogeoop.base.business.mongodb.EnhanceBaseModelManager;
import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;
import io.github.chaogeoop.base.business.redis.KeyEntity;
import io.github.chaogeoop.base.business.redis.StrictRedisProvider;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.redis.KeyType;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import javax.annotation.Nullable;
import javax.lang.model.type.NullType;
import java.util.*;
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

        EnhanceBaseModelManager.getBaseCollectionNameByClazz(this.mongoTemplate, this.logDbClazz);
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

                MonitorLog log = mongoTemplate.findOne(query, logDbClazz);
                if (log != null) {
                    for (NameCount nameCount : log.getDatabase()) {
                        log.getDatabaseTmp().put(nameCount.getName(), nameCount.getCount());
                    }

                    for (NameCount nameCount : log.getCount()) {
                        log.getCountTmp().put(nameCount.getName(), nameCount.getCount());
                    }

                    for (NameCount nameCount : log.getRedis()) {
                        log.getRedisTmp().put(nameCount.getName(), nameCount.getCount());
                    }
                }

                return log;
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
                data.setDatabaseTmp(newLog.getDatabase());
                data.setCountTmp(newLog.getCount());
                data.setRedisTmp(newLog.getRedis());
                data.setCreated(new Date());

                return data;
            }
        };

        entity.getDefault(this.redisAbout.getStrictRedisProvider(), existDbLog -> {
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
                    Long oldValue = existDbLog.getDatabaseTmp().get(entry.getKey());
                    if (oldValue == null || oldValue < entry.getValue()) {
                        existDbLog.getDatabaseTmp().put(entry.getKey(), entry.getValue());
                        needSave = true;
                    }
                }

                for (Map.Entry<String, Long> entry : newLog.getCount().entrySet()) {
                    Long oldValue = existDbLog.getCountTmp().get(entry.getKey());
                    if (oldValue == null || oldValue < entry.getValue()) {
                        existDbLog.getCountTmp().put(entry.getKey(), entry.getValue());
                        needSave = true;
                    }
                }

                for (Map.Entry<String, Long> entry : newLog.getRedis().entrySet()) {
                    Long oldValue = existDbLog.getRedisTmp().get(entry.getKey());
                    if (oldValue == null || oldValue < entry.getValue()) {
                        existDbLog.getRedisTmp().put(entry.getKey(), entry.getValue());
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
        private StrictRedisProvider strictRedisProvider;

        private M ioMonitorPersistLockType;

        public static <M extends KeyType> RedisAbout<M> of(StrictRedisProvider strictRedisProvider, M lockType) {
            RedisAbout<M> data = new RedisAbout<>();

            data.setStrictRedisProvider(strictRedisProvider);
            data.setIoMonitorPersistLockType(lockType);

            return data;
        }
    }

    @Setter
    @Getter
    public static class MonitorLog extends BaseModel {
        @Indexed(unique = true)
        private String funcName;

        @Transient
        private Map<String, Long> databaseTmp = new HashMap<>();

        @Transient
        private Map<String, Long> countTmp = new HashMap<>();

        @Transient
        private Map<String, Long> redisTmp = new HashMap<>();

        private List<NameCount> database = new ArrayList<>();

        private List<NameCount> count = new ArrayList<>();

        private List<NameCount> redis = new ArrayList<>();

        @Indexed
        private Long databaseTotal = 0L;

        @Indexed
        private Long countTotal = 0L;

        @Indexed
        private Long redisTotal = 0L;

        private Date created = new Date();

        public void calTotal() {
            this.database.clear();
            this.count.clear();
            this.redis.clear();
            this.databaseTotal = 0L;
            this.countTotal = 0L;
            this.redisTotal = 0L;

            for (Map.Entry<String, Long> entry : this.databaseTmp.entrySet()) {
                this.databaseTotal += entry.getValue();
                this.database.add(NameCount.of(entry.getKey(), entry.getValue()));
            }

            for (Map.Entry<String, Long> entry : this.countTmp.entrySet()) {
                this.countTotal += entry.getValue();
                this.count.add(NameCount.of(entry.getKey(), entry.getValue()));
            }

            for (Map.Entry<String, Long> entry : this.redisTmp.entrySet()) {
                this.redisTotal += entry.getValue();
                this.redis.add(NameCount.of(entry.getKey(), entry.getValue()));
            }
        }
    }

    @Setter
    @Getter
    public static class NameCount {
        private String name;

        private Long count;

        public static NameCount of(String name, Long count) {
            NameCount data = new NameCount();

            data.setName(name);
            data.setCount(count);

            return data;
        }
    }
}
