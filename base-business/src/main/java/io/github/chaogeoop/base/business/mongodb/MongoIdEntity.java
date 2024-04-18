package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;
import io.github.chaogeoop.base.business.mongodb.basic.IUidGenerator;
import io.github.chaogeoop.base.business.common.errors.BizException;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MongoIdEntity {
    private final MongoTemplate mongoTemplate;

    private final Class<? extends EnhanceBaseModel> clazz;

    private final IUidGenerator uidGenerator;

    private static final ConcurrentHashMap<MongoTemplate, IdCache> databaseIdCacheMap = new ConcurrentHashMap<>();


    private MongoIdEntity(
            MongoTemplate mongoTemplate, Class<? extends EnhanceBaseModel> clazz, @Nullable IUidGenerator uidGenerator
    ) {
        this.mongoTemplate = mongoTemplate;
        this.clazz = clazz;
        this.uidGenerator = uidGenerator;

        this.initDatabaseIdCacheMap(this.mongoTemplate);

        if (this.uidGenerator == null) {
            EnhanceBaseModel.getBaseCollectionNameByClazz(this.mongoTemplate, Uid.class);
        }
    }

    private void initDatabaseIdCacheMap(MongoTemplate mongoTemplate) {
        if (databaseIdCacheMap.containsKey(mongoTemplate)) {
            return;
        }

        synchronized (MongoTemplate.class) {
            if (databaseIdCacheMap.containsKey(mongoTemplate)) {
                return;
            }

            databaseIdCacheMap.put(mongoTemplate, new IdCache());
        }
    }

    public static MongoIdEntity of(
            MongoTemplate mongoTemplate, Class<? extends EnhanceBaseModel> clazz, @Nullable IUidGenerator uidGenerator
    ) {
        return new MongoIdEntity(mongoTemplate, clazz, uidGenerator);
    }


    public long nextUid() {
        List<Long> ids = this.nextUids(1);

        return ids.get(0);
    }

    public long nextUid(boolean useCache) {
        if (!useCache) {
            return this.nextUid();
        }

        String nameInIdCollection = BaseModel.getNameInIdCollectionByClazz(this.clazz);
        IdCache idCacheContext = databaseIdCacheMap.get(this.mongoTemplate);

        long id = idCacheContext.get(nameInIdCollection);
        if (id >= 0) {
            return id;
        }

        synchronized (String.format("%s_uidGeneratorLock", nameInIdCollection).intern()) {
            id = idCacheContext.get(nameInIdCollection);
            if (id >= 0) {
                return id;
            }

            List<Long> ids = this.nextUids(1000);
            idCacheContext.put(nameInIdCollection, ids.get(0), ids.get(ids.size() - 1));

            id = idCacheContext.get(nameInIdCollection);
        }

        return id;
    }


    public List<Long> nextUids(int count) {
        String nameInIdCollection = BaseModel.getNameInIdCollectionByClazz(this.clazz);

        if (this.uidGenerator != null) {
            return this.uidGenerator.getUids(this.clazz, nameInIdCollection, count);
        }

        return defaultGetUids(this.mongoTemplate, this.clazz, count);
    }

    public static List<Long> defaultGetUids(MongoTemplate mongoTemplate, Class<? extends EnhanceBaseModel> clazz, int count) {
        EnhanceBaseModel.getBaseCollectionNameByClazz(mongoTemplate, Uid.class);

        String nameInIdCollection = BaseModel.getNameInIdCollectionByClazz(clazz);

        List<Long> ids = new ArrayList<>();

        Query query = new Query();
        query.addCriteria(Criteria.where("name").is(nameInIdCollection));

        Update update = new Update();
        update.inc("uid", count);

        FindAndModifyOptions option = new FindAndModifyOptions();
        option.returnNew(true).upsert(true);

        Uid result = mongoTemplate.findAndModify(query, update, option, Uid.class);
        if (result == null || result.getUid() == null) {
            throw new BizException("获取ID错误");
        }

        long afterUid = result.getUid();

        for (int i = 1; i <= count; i++) {
            ids.add(afterUid - count + i);
        }

        return ids;
    }


    private static class IdCache {
        private final ConcurrentHashMap<String, IdGenerator> idCache = new ConcurrentHashMap<>();

        public long get(String nameInIdCollection) {
            IdGenerator idGenerator = idCache.get(nameInIdCollection);
            if (idGenerator == null) {
                return -1L;
            }

            long newId = idGenerator.next.getAndIncrement();
            if (newId > idGenerator.limit) {
                return -1L;
            }

            return newId;
        }

        public void put(String nameInIdCollection, long start, long limit) {
            IdGenerator idGenerator = IdGenerator.of(start, limit);

            idCache.put(nameInIdCollection, idGenerator);
        }
    }


    @Setter
    @Getter
    private static class IdGenerator {
        private AtomicLong next;

        private long limit;

        public static IdGenerator of(long start, long limit) {
            if (start >= limit) {
                throw new BizException("创建id生成器错误");
            }

            IdGenerator data = new IdGenerator();

            data.setNext(new AtomicLong(start));
            data.setLimit(limit);

            return data;
        }
    }

    @Setter
    @Getter
    @Document("defaultdocumentuids")
    public static class Uid extends EnhanceBaseModel {
        @Indexed(unique = true)
        private String name;

        private Long uid;
    }
}
