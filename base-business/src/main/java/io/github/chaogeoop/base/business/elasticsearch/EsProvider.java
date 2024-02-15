package io.github.chaogeoop.base.business.elasticsearch;

import io.github.chaogeoop.base.business.common.entities.EsPageSplitter;
import io.github.chaogeoop.base.business.common.interfaces.CheckResourceValidToHandleInterface;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.mongodb.MongoPersistEntity;
import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.redis.DistributedKeyType;
import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.searchbox.client.JestClient;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.index.query.QueryBuilder;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.MultiValueMap;

import javax.lang.model.type.NullType;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class EsProvider implements MongoPersistEntity.AfterDbPersistInterface {
    private final MongoTemplate mongoTemplate;
    private final RedisAbout<?> redisAbout;
    private final Function<List<EsNameId>, NullType> esSyncSender;
    private final JestClient jestClient;
    private final Class<? extends SyncLog> logDbClazz;

    public static final ConcurrentHashMap<MongoTemplate, JestClient> databaseEsMap = new ConcurrentHashMap<>();


    public EsProvider(
            MongoTemplate mongoTemplate,
            RedisAbout<?> redisAbout,
            Function<List<EsNameId>, NullType> esSyncSender,
            JestClient jestClient,
            Class<? extends SyncLog> logDbClazz
    ) {
        this.mongoTemplate = mongoTemplate;
        this.redisAbout = redisAbout;
        this.esSyncSender = esSyncSender;
        this.jestClient = jestClient;
        this.logDbClazz = logDbClazz;

        databaseEsMap.put(this.mongoTemplate, this.jestClient);

        BaseModel.getBaseCollectionNameByClazz(this.mongoTemplate, this.logDbClazz);
    }

    public MongoTemplate giveMongoTemplate() {
        return this.mongoTemplate;
    }

    public JestClient giveEs() {
        return this.jestClient;
    }

    @Override
    public void handle(MongoPersistEntity.PersistMap persistMap) {
        if (this.mongoTemplate != persistMap.getMongoTemplate()) {
            throw new BizException("使用了错误的mongo数据库!");
        }

        List<SyncLog> logs = new ArrayList<>();

        Map<ActionEnum, MultiValueMap<MongoPersistEntity.ModelClazzCollectionName, BaseModel>> map = new HashMap<>();
        map.put(ActionEnum.INSERT, persistMap.getInsertMap());
        map.put(ActionEnum.UPDATE, persistMap.getSaveMap());
        map.put(ActionEnum.DELETE, persistMap.getDeleteMap());

        Set<EsHelper.EsUnitInfo> esUnitInfoSet = new HashSet<>();

        for (Map.Entry<ActionEnum, MultiValueMap<MongoPersistEntity.ModelClazzCollectionName, BaseModel>> actionEntry : map.entrySet()) {
            for (Map.Entry<MongoPersistEntity.ModelClazzCollectionName, List<BaseModel>> entry : actionEntry.getValue().entrySet()) {
                if (!ISearch.class.isAssignableFrom(entry.getKey().getModelClazz())) {
                    continue;
                }
                if (entry.getValue().isEmpty()) {
                    continue;
                }

                EsHelper.EsUnitInfo esUnitInfo = EsHelper.EsUnitInfo.of((ISearch<? extends BaseEs>) entry.getValue().get(0), this.jestClient);
                esUnitInfoSet.add(esUnitInfo);

                for (BaseModel obj : entry.getValue()) {
                    BaseEs esData = ((ISearch<? extends BaseEs>) obj).giveEsData();
                    if (esData == null) {
                        continue;
                    }

                    SyncLog log;
                    try {
                        log = this.logDbClazz.getDeclaredConstructor().newInstance();
                    } catch (Exception e) {
                        throw new BizException("创建EsSyncLog错误");
                    }

                    log.setBaseEsName(esUnitInfo.getBaseEsName());
                    log.setEsName(esUnitInfo.getEsName());
                    log.setMapping(esUnitInfo.getMapping());
                    log.setUniqueId(entry.getKey().getCollectionName().toLowerCase() + "_" + obj.getId().toString(16));
                    log.setVersion(obj.getV());
                    log.setAction(actionEntry.getKey());
                    log.setData(JsonHelper.writeValueAsString(esData));

                    logs.add(log);
                }
            }
        }

        if (logs.isEmpty()) {
            return;
        }

        List<SyncLog> results = (List<SyncLog>) this.mongoTemplate.insert(logs, this.logDbClazz);
        List<EsNameId> list = CollectionHelper.map(results, EsNameId::of);

        MongoPersistEntity.MessageInterface message = new MongoPersistEntity.MessageInterface() {
            @Override
            public void send() {
                for (EsHelper.EsUnitInfo esUnit : esUnitInfoSet) {
                    BaseEs.getAccordEsNameByData(jestClient, esUnit.getBaseEsName(), esUnit.getEsName(), esUnit.getMapping());
                }

                esSyncSender.apply(list);
            }
        };

        persistMap.getMessages().add(message);
    }

    public void syncToEs(List<EsNameId> messages) {
        if (messages.size() == 1) {
            this.syncToEs(messages.get(0));
            return;
        }

        for (EsNameId message : messages) {
            this.esSyncSender.apply(Lists.newArrayList(message));
        }
    }

    private void syncToEs(EsNameId logJudge) {
        CheckResourceValidToHandleInterface<SyncLog> entity = new CheckResourceValidToHandleInterface<>() {
            @Override
            public DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> getLock() {
                return DistributedKeyProvider.KeyEntity.of(
                        redisAbout.getEsDataSyncLockType(),
                        logJudge.calTypeId()
                );
            }

            @Override
            public SyncLog findResource() {
                Query query = new Query();
                query.addCriteria(Criteria.where("esName").is(logJudge.getEsName()));
                query.addCriteria(Criteria.where("uniqueId").is(logJudge.getUniqueId()));

                query.with(Sort.by(Sort.Order.desc("version"), Sort.Order.desc("_id")));

                return mongoTemplate.findOne(query, logDbClazz);
            }

            @Override
            public boolean validToHandle(SyncLog log) {
                return true;
            }
        };

        entity.handle(this.redisAbout.getRedisProvider(), o -> {
            syncToEs(o);

            Query query = new Query();
            query.addCriteria(Criteria.where("esName").is(logJudge.getEsName()));
            query.addCriteria(Criteria.where("uniqueId").is(logJudge.getUniqueId()));
            query.addCriteria(Criteria.where("version").lte(o.getVersion()));

            this.mongoTemplate.remove(query, this.logDbClazz);

            return null;
        }, o -> null);
    }

    private <M extends SyncLog> void syncToEs(M log) {
        String realEsName = BaseEs.getAccordEsNameByData(this.jestClient, log.getBaseEsName(), log.getEsName(), log.getMapping());

        if (ActionEnum.DELETE.equals(log.getAction())) {
            SimpleSearchHelper.deleteData(this.jestClient, realEsName, log.getUniqueId());
            return;
        }

        DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity = DistributedKeyProvider.KeyEntity.of(
                this.redisAbout.getEsDataCacheType(),
                log.calTypeId()
        );

        if (EsProvider.ActionEnum.UPDATE.equals(log.getAction())) {
            String cacheStringData = this.redisAbout.getRedisProvider().get(keyEntity, String.class);
            if (cacheStringData != null && cacheStringData.equals(log.getData())) {
                return;
            }
        }

        SimpleSearchHelper.insertOrUpdateData(this.jestClient, realEsName, log.getUniqueId(), log.getData());

        this.redisAbout.getRedisProvider().set(keyEntity, RedisProvider.AcceptType.of(log.getData()), Duration.ofHours(6));
    }


    public <E extends BaseEs, M extends ISearch<E>> ListPage<E> pageQuery(QueryBuilder queryBuilder, EsPageSplitter esPageSplitter, List<M> judgeKeys) {
        if (judgeKeys.isEmpty()) {
            return ListPage.of(esPageSplitter.getOffset(), esPageSplitter.getLimit(), 0, new ArrayList<>());
        }

        List<EsHelper.EsUnitInfo> esUnitInfos = CollectionHelper.map(judgeKeys, o -> EsHelper.EsUnitInfo.of(o, this.jestClient));
        Set<String> realEsNames = CollectionHelper.map(Sets.newHashSet(esUnitInfos),
                o -> BaseEs.getAccordEsNameByData(this.jestClient, o.getBaseEsName(), o.getEsName(), o.getMapping())
        );

        return SimpleSearchHelper.pageQuery(this.jestClient, queryBuilder, esPageSplitter, realEsNames, judgeKeys.get(0).giveEsModel());
    }

    public <M extends ISearch<? extends BaseEs>> long count(QueryBuilder queryBuilder, List<M> judgeKeys) {
        if (judgeKeys.isEmpty()) {
            return 0;
        }

        List<EsHelper.EsUnitInfo> esUnitInfos = CollectionHelper.map(judgeKeys, o -> EsHelper.EsUnitInfo.of(o, this.jestClient));
        Set<String> realEsNames = CollectionHelper.map(Sets.newHashSet(esUnitInfos),
                o -> BaseEs.getAccordEsNameByData(this.jestClient, o.getBaseEsName(), o.getEsName(), o.getMapping())
        );

        return SimpleSearchHelper.count(this.jestClient, queryBuilder, realEsNames);
    }

    //不建议使用,只是为了单机测试
    public void deleteIndex(Class<? extends ISearch<? extends BaseEs>> clazz) {
        BaseEs.deleteIndex(this.jestClient, clazz);
    }

    @Setter
    @Getter
    public static class RedisAbout<M extends DistributedKeyType> {
        private RedisProvider redisProvider;

        private M esDataCacheType;

        private M esDataSyncLockType;

        public static <M extends DistributedKeyType> RedisAbout<M> of(
                RedisProvider redisProvider, M cacheType, M lockType
        ) {
            RedisAbout<M> data = new RedisAbout<>();

            data.setRedisProvider(redisProvider);
            data.setEsDataCacheType(cacheType);
            data.setEsDataSyncLockType(lockType);

            return data;
        }
    }

    @Setter
    @Getter
    @CompoundIndexes({
            @CompoundIndex(name = "esName_uniqueId", def = "{'esName':1, 'uniqueId':1}"),
    })
    public static class SyncLog extends BaseModel {
        private String baseEsName;

        private String esName;

        private String mapping;

        private String uniqueId;

        private Long version;

        private ActionEnum action;

        private String data;

        public void setVersion(Long version) {
            if (version == null) {
                version = 0L;
            }

            this.version = version;
        }

        public String calTypeId() {
            return EsNameId.of(this).calTypeId();
        }
    }

    @Setter
    @Getter
    public static class EsNameId {
        private String esName;

        private String uniqueId;

        private String calTypeId() {
            return String.format("%s-%s", this.esName, this.uniqueId);
        }

        public static <T extends SyncLog> EsNameId of(T log) {
            EsNameId data = new EsNameId();

            data.setEsName(log.getEsName());
            data.setUniqueId(log.getUniqueId());

            return data;
        }
    }

    public enum ActionEnum {
        INSERT, UPDATE, DELETE
    }
}
