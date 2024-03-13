package io.github.chaogeoop.base.business.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.chaogeoop.base.business.common.interfaces.CheckResourceValidToHandleInterface;
import io.github.chaogeoop.base.business.common.interfaces.DefaultResourceInterface;
import io.github.chaogeoop.base.business.mongodb.*;
import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.KeyEntity;
import io.github.chaogeoop.base.business.redis.StrictRedisProvider;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.redis.KeyType;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.common.helpers.DateHelper;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import javax.annotation.Nullable;
import javax.lang.model.type.NullType;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;


@Slf4j
public class CommonCountProvider {
    private final PersistProvider persistProvider;
    private final MongoTemplate mongoTemplate;
    private final RedisAbout<? extends KeyType> redisAbout;
    private final Function<List<String>, NullType> countHistorySender;
    private final Class<? extends CommonCountTotal> totalDbClazz;
    private final Class<? extends CommonCountDateLog> dateLogDbClazz;

    private static final String COMMON_COUNT_LUA = "local beforeKey = KEYS[1]  \n" +
            "local nextKey = KEYS[2]  \n" +
            "local afterAllKey = KEYS[3]  \n" +
            "local nextInc = tonumber(ARGV[1])  \n" +
            "local total = tonumber(ARGV[2])  \n" +
            "local timeout = tonumber(ARGV[3])  \n" +
            "if beforeKey ~= nextKey then  \n" +
            "    if redis.call(\"EXISTS\", beforeKey) == 1 then  \n" +
            "        local beforeValue = tonumber(redis.call(\"GET\", beforeKey))  \n" +
            "        if beforeValue ~= nil then  \n" +
            "            total = total + beforeValue  \n" +
            "        end  \n" +
            "    end  \n" +
            "    if nextInc > 0 then  \n" +
            "        redis.call(\"INCRBY\", nextKey, nextInc)  \n" +
            "        total = total + nextInc  \n" +
            "    end  \n" +
            "else  \n" +
            "    if nextInc ~= 0 then  \n" +
            "        local afterCache = redis.call(\"INCRBY\", nextKey, nextInc) \n" +
            "        total = total + afterCache \n" +
            "    else  \n" +
            "        if redis.call(\"EXISTS\", nextKey) == 1 then  \n" +
            "            local nextValue = tonumber(redis.call(\"GET\", nextKey))  \n" +
            "            if nextValue ~= nil then  \n" +
            "                total = total + nextValue  \n" +
            "            end  \n" +
            "        end  \n" +
            "    end  \n" +
            "end  \n" +
            "redis.call(\"SETEX\", afterAllKey, timeout, tostring(total))  \n" +
            "return total";


    public CommonCountProvider(
            PersistProvider persistProvider,
            RedisAbout<? extends KeyType> redisAbout,
            Function<List<String>, NullType> countHistorySender,
            Class<? extends CommonCountTotal> totalDbClazz,
            Class<? extends CommonCountDateLog> dateLogDbClazz
    ) {
        this.persistProvider = persistProvider;
        this.mongoTemplate = this.persistProvider.giveMongoTemplate();
        this.redisAbout = redisAbout;
        this.countHistorySender = countHistorySender;
        this.totalDbClazz = totalDbClazz;
        this.dateLogDbClazz = dateLogDbClazz;
    }

    public Map<CountBiz, Long> getBizTotalMap(Set<CountBiz> bizList) {
        List<CountBiz> list = Lists.newArrayList(bizList);

        return this.redisAbout.getStrictRedisProvider().getMapFromValueCache(
                this.redisAbout.getCountBizAfterAllTotalCacheKeyType(),
                this.redisAbout.getAfterAllTotalCacheDuration(),
                Long.class,
                list,
                needCacheList -> getBizTotalMapWithoutCache(Sets.newHashSet(needCacheList))
        );
    }

    public Map<CountBiz, Long> getBizTotalMapWithoutCache(Set<CountBiz> bizList) {
        Map<CountBiz, Long> result = new HashMap<>();

        Map<CountBiz, CommonCountTotal> bizCountTotalMap = getBizCountTotalMap(bizList);

        List<CountBizDate> hasTotalBizDateList = new ArrayList<>();
        for (CountBiz biz : bizList) {
            CommonCountTotal log = bizCountTotalMap.get(biz);
            if (log == null) {
                result.put(biz, 0L);
                continue;
            }

            hasTotalBizDateList.add(biz.convertToBizDate(log.getLatestCacheDate()));
        }

        List<KeyEntity<? extends KeyType>> latestCacheKeys = CollectionHelper.map(
                hasTotalBizDateList,
                o -> KeyEntity.of(redisAbout.getCountBizDateCacheKeyType(), o.giveJson())
        );

        List<Long> cacheBizDateTotals = redisAbout.getStrictRedisProvider().multiGet(latestCacheKeys, Long.class);

        for (int i = 0; i < hasTotalBizDateList.size(); i++) {
            CountBizDate bizDate = hasTotalBizDateList.get(i);
            Long bizDateCacheTotal = cacheBizDateTotals.get(i);
            if (bizDateCacheTotal == null) {
                bizDateCacheTotal = 0L;
            }

            CountBiz biz = bizDate.extractBiz();

            long bizTotal = bizDateCacheTotal;

            CommonCountTotal log = bizCountTotalMap.get(biz);
            if (log != null) {
                bizTotal += log.getTotal();
            }

            result.put(biz, bizTotal);
        }

        return result;
    }

    public Map<CountBiz, Map<String, Long>> getBizLogsMap(Set<CountBiz> bizList, List<String> dates) {
        Map<CountBiz, Map<String, Long>> map = new HashMap<>();
        if (CollectionHelper.isEmpty(bizList)) {
            return map;
        }

        List<CountBizDate> bizDates = new ArrayList<>();
        dates = CollectionHelper.unique(dates);
        dates.sort(Comparator.comparing(o -> DateHelper.parseStringDate(o, DateHelper.DateFormatEnum.fullUntilDay)));

        for (CountBiz biz : bizList) {
            map.put(biz, new LinkedHashMap<>());
            for (String date : dates) {
                map.get(biz).put(date, 0L);
                bizDates.add(biz.convertToBizDate(date));
            }
        }


        Map<String, List<CountBizDate>> collectionNameBizDatesMap = CollectionHelper.groupBy(
                bizDates, o -> BaseModel.getAccordCollectionNameByData(this.mongoTemplate, CommonCountDateLog.splitKeyOf(this.dateLogDbClazz, o))
        );

        List<CommonCountDateLog> dateLogs = new ArrayList<>();


        for (Map.Entry<String, List<CountBizDate>> entry : collectionNameBizDatesMap.entrySet()) {
            List<Criteria> orList = new ArrayList<>();
            for (CountBizDate bizDate : entry.getValue()) {
                orList.add(Criteria.where("t").is(bizDate.getTypeId()).and("b").is(bizDate.getBizType())
                        .and("s").is(bizDate.getSubBizType()).and("d").is(bizDate.getDate()));
            }

            Query query = new Query();
            query.addCriteria(new Criteria().orOperator(orList.toArray(new Criteria[0])));

            dateLogs.addAll(this.mongoTemplate.find(query, this.dateLogDbClazz, entry.getKey()));
        }

        List<CountBizDate> needReadCacheBizDateList = new ArrayList<>();
        Map<CountBiz, CommonCountTotal> bizCountTotalMap = getBizCountTotalMap(bizList);
        for (CountBiz biz : bizList) {
            CommonCountTotal log = bizCountTotalMap.get(biz);
            if (log == null || !dates.contains(log.getLatestCacheDate())) {
                continue;
            }

            needReadCacheBizDateList.add(biz.convertToBizDate(log.getLatestCacheDate()));
        }

        if (!needReadCacheBizDateList.isEmpty()) {
            List<KeyEntity<? extends KeyType>> needReadCacheKeys = CollectionHelper.map(
                    needReadCacheBizDateList,
                    o -> KeyEntity.of(redisAbout.getCountBizDateCacheKeyType(), o.giveJson())
            );
            List<Long> values = redisAbout.getStrictRedisProvider().multiGet(needReadCacheKeys, Long.class);
            for (int i = 0; i < needReadCacheBizDateList.size(); i++) {
                CountBizDate bizDate = needReadCacheBizDateList.get(i);
                Long value = values.get(i);
                if (value == null) {
                    continue;
                }

                CommonCountDateLog dateLog = CommonCountDateLog.of(dateLogDbClazz, bizDate);
                dateLog.setTotal(value);

                dateLogs.add(dateLog);
            }
        }

        for (CommonCountDateLog dateLog : dateLogs) {
            map.get(dateLog.extractBiz()).put(dateLog.getDate(), dateLog.getTotal());
        }

        return map;
    }

    private Map<CountBiz, CommonCountTotal> getBizCountTotalMap(Set<CountBiz> bizList) {
        Map<CountBiz, CommonCountTotal> map = new HashMap<>();

        Map<String, List<CountBiz>> collectionNameBizListMap = CollectionHelper.groupBy(
                bizList, o -> BaseModel.getAccordCollectionNameByData(this.mongoTemplate, CommonCountTotal.splitKeyOf(this.totalDbClazz, o))
        );

        for (Map.Entry<String, List<CountBiz>> entry : collectionNameBizListMap.entrySet()) {
            List<Criteria> orList = new ArrayList<>();
            for (CountBiz biz : entry.getValue()) {
                orList.add(Criteria.where("t").is(biz.getTypeId()).and("b").is(biz.getBizType()).and("s").is(biz.getSubBizType()));
            }
            Query query = new Query();
            query.addCriteria(new Criteria().orOperator(orList.toArray(new Criteria[0])));

            List<? extends CommonCountTotal> records = this.mongoTemplate.find(query, totalDbClazz, entry.getKey());
            for (CommonCountTotal record : records) {
                map.put(record.extractBiz(), record);
            }
        }

        return map;
    }

    public void freezeColdData(int intervalDays) {
        if (intervalDays < 1) {
            throw new BizException(String.format("intervalDays cant lt 1: %s", intervalDays));
        }

        Date currentTime = new Date();
        String currentDate = DateHelper.dateToString(currentTime, DateHelper.DateFormatEnum.fullUntilDay);
        Date limitTime = DateHelper.plusDurationOfDate(currentTime, Duration.ofDays(intervalDays * -1));

        Set<String> collectionNames = this.mongoTemplate.getCollectionNames();
        Set<String> totalRelativeCollectionNames = CollectionHelper.find(collectionNames, o -> SplitCollectionHelper.isClazzRelativeCollection(o, totalDbClazz));
        if (totalRelativeCollectionNames.isEmpty()) {
            return;
        }

        for (String collectionName : totalRelativeCollectionNames) {
            BigInteger _id = null;

            while (true) {
                Query query = new Query();
                query.addCriteria(Criteria.where("sc").is(this.redisAbout.getStrictRedisProvider().getDistributedKeyProvider().getScope()));
                query.addCriteria(Criteria.where("st").lt(limitTime.getTime()));
                query.addCriteria(Criteria.where("c").is(false));
                query.limit(1000);
                query.with(Sort.by(Sort.Order.asc("_id")));
                if (_id != null) {
                    query.addCriteria(Criteria.where("_id").gt(new ObjectId(_id.toString(16))));
                }

                List<? extends CommonCountTotal> logs = this.mongoTemplate.find(query, this.totalDbClazz, collectionName);
                if (logs.isEmpty()) {
                    break;
                }

                CommonCountTotal lastRecord = logs.get(logs.size() - 1);
                _id = lastRecord.getId();

                List<? extends CommonCountTotal> distributeSafeLogs = CollectionHelper.remove(logs, o -> o.getLockFinder() != null);

                if (!logs.isEmpty()) {
                    Map<CountBizDate, Long> bizDateIncMap = new HashMap<>();
                    for (CommonCountTotal log : logs) {
                        bizDateIncMap.put(log.extractBiz().convertToBizDate(currentDate), 0L);
                    }

                    MongoPersistEntity.PersistEntity persistEntity = this.insertPersistHistory(bizDateIncMap);
                    this.persistProvider.persist(Lists.newArrayList(persistEntity));
                }

                Map<CountLock, List<CommonCountTotal>> group = CountLock.group((List<CommonCountTotal>) distributeSafeLogs);
                for (Map.Entry<CountLock, List<CommonCountTotal>> entry : group.entrySet()) {
                    Map<CountBiz, Long> bizIncMap = new HashMap<>();
                    for (CommonCountTotal commonCountTotal : entry.getValue()) {
                        bizIncMap.put(commonCountTotal.extractBiz(), 0L);
                    }

                    KeyType keyType = this.redisAbout.getStrictRedisProvider().getDistributedKeyProvider().getKeyType(entry.getKey().getLockFinder());
                    if (keyType == null) {
                        continue;
                    }
                    KeyEntity<KeyType> lock = KeyEntity.of(keyType, entry.getKey().getLockId());

                    try {
                        this.redisAbout.getStrictRedisProvider().exeFuncWithLock(lock, o -> {
                            Pair<MongoPersistEntity.PersistEntity, Map<CountBiz, CountBizEntity>> pair = this.distributeSafeMultiBizCount(bizIncMap, new Date(), lock);
                            this.persistProvider.persist(Lists.newArrayList(pair.getLeft()));
                            return null;
                        });
                    } catch (Exception e) {
                        log.error("freezeColdData error", e);
                    }
                }
            }
        }
    }

    public MongoPersistEntity.PersistEntity insertPersistHistoryNow(
            Map<CountBiz, Long> bizIncMap
    ) {
        String date = DateHelper.dateToString(DateHelper.plusDurationOfDate(new Date(), Duration.ofSeconds(-10)), DateHelper.DateFormatEnum.fullUntilDay);

        Map<CountBizDate, Long> bizDateIncMap = new HashMap<>();
        for (Map.Entry<CountBiz, Long> entry : bizIncMap.entrySet()) {
            bizDateIncMap.put(entry.getKey().convertToBizDate(date), entry.getValue());
        }

        return this.insertPersistHistory(bizDateIncMap);
    }

    public MongoPersistEntity.PersistEntity insertPersistHistory(Map<CountBizDate, Long> bizDateIncMap) {
        return this.insertPersistHistory(Lists.newArrayList(bizDateIncMap));
    }

    public MongoPersistEntity.PersistEntity insertPersistHistory(List<Map<CountBizDate, Long>> bizDateIncMapList) {
        MongoPersistEntity.PersistEntity persistEntity = new MongoPersistEntity.PersistEntity();

        Map<String, StrictRedisProvider.AcceptType> map = new HashMap<>();
        for (Map<CountBizDate, Long> bizDateIncMap : bizDateIncMapList) {
            for (Map.Entry<CountBizDate, Long> entry : bizDateIncMap.entrySet()) {
                CommonCountPersistHistory data = CommonCountPersistHistory.of(entry.getKey(), entry.getValue());
                String key = UUID.randomUUID().toString();

                map.put(key, data.toRedisAccept());
            }
        }

        MongoPersistEntity.CacheInterface historyStoreCache = new MongoPersistEntity.CacheInterface() {
            @Override
            public void persist() {
                redisAbout.getStrictRedisProvider().hmset(redisAbout.getCountHistoryHashKey(), map);
            }

            @Override
            public void rollback() {
                redisAbout.getStrictRedisProvider().hdel(redisAbout.getCountHistoryHashKey(), map.keySet());
            }
        };

        MongoPersistEntity.MessageInterface message = new MongoPersistEntity.MessageInterface() {
            @Override
            public void send() {
                countHistorySender.apply(Lists.newArrayList(map.keySet()));
            }
        };

        persistEntity.getCacheList().add(historyStoreCache);
        persistEntity.getMessages().add(message);

        return persistEntity;
    }

    public void historyToCount(List<String> ids) {
        if (ids.size() == 1) {
            this.persistCountByHistoryId(ids.get(0));
            return;
        }

        for (String id : ids) {
            this.countHistorySender.apply(Lists.newArrayList(id));
        }
    }


    public void persistCountByHistoryId(String id) {
        CheckResourceValidToHandleInterface<CommonCountPersistHistory> entity = new CheckResourceValidToHandleInterface<>() {
            @Override
            public KeyEntity<? extends KeyType> getLock() {
                return KeyEntity.of(redisAbout.getCommonCountPersistHistoryLockType(), id);
            }

            @Override
            public CommonCountPersistHistory findResource() {
                return redisAbout.getStrictRedisProvider().hget(redisAbout.getCountHistoryHashKey(), id, CommonCountPersistHistory.class);
            }

            @Override
            public boolean validToHandle(CommonCountPersistHistory resource) {
                return true;
            }
        };

        entity.handle(this.redisAbout.getStrictRedisProvider(), o -> {
            CountBizDate bizDate = o.extractBizDate();
            CountBiz biz = bizDate.extractBiz();

            CountBizEntity countBizEntity = new CountBizEntity(bizDate, o.getTotal());
            MongoPersistEntity.PersistEntity persistEntity = new MongoPersistEntity.PersistEntity();


            MongoPersistEntity.CacheInterface deleteHistoryCache = new MongoPersistEntity.CacheInterface() {
                @Override
                public void persist() {
                    redisAbout.getStrictRedisProvider().hdel(redisAbout.getCountHistoryHashKey(), Sets.newHashSet(id));
                }

                @Override
                public void rollback() {
                    redisAbout.getStrictRedisProvider().hmset(redisAbout.getCountHistoryHashKey(), Map.of(id, o.toRedisAccept()));
                }
            };

            if (!countBizEntity.needLock) {
                countBizEntity.initCacheAbout();
                countBizEntity.setCommonCountDateLog();
                Map<KeyEntity<? extends KeyType>, Long> deleteRollbackMap = countBizEntity.collectNeedPersist(persistEntity);
                MongoPersistEntity.CacheInterface deleteRollbackCache = this.getDeleteRollbackCache(deleteRollbackMap);
                if (deleteRollbackCache != null) {
                    persistEntity.getCacheList().add(deleteRollbackCache);
                }

                persistEntity.getCacheList().add(deleteHistoryCache);

                this.persistProvider.persist(Lists.newArrayList(persistEntity));

                return null;
            }

            this.redisAbout.getStrictRedisProvider().exeFuncWithLock(
                    KeyEntity.of(this.redisAbout.getCommonCountTotalCreateLockType(), biz.giveJson()),
                    M -> {
                        countBizEntity.setCommonCountTotal();
                        countBizEntity.initCacheAbout();
                        countBizEntity.setCommonCountDateLog();
                        Map<KeyEntity<? extends KeyType>, Long> deleteRollbackMap = countBizEntity.collectNeedPersist(persistEntity);
                        MongoPersistEntity.CacheInterface deleteRollbackCache = this.getDeleteRollbackCache(deleteRollbackMap);
                        if (deleteRollbackCache != null) {
                            persistEntity.getCacheList().add(deleteRollbackCache);
                        }

                        persistEntity.getCacheList().add(deleteHistoryCache);

                        this.persistProvider.persist(Lists.newArrayList(persistEntity));

                        return null;
                    }
            );

            if (CacheStateEnum.NO_CACHE.equals(countBizEntity.cacheState) && countBizEntity.inc > 0) {
                MongoPersistEntity.PersistEntity ensureAfterAllTotalEntity = this.insertPersistHistoryNow(Map.of(biz, 0L));

                this.persistProvider.persist(Lists.newArrayList(ensureAfterAllTotalEntity));
            }

            return null;
        }, o -> null);
    }


    public Pair<MongoPersistEntity.PersistEntity, Map<CountBiz, CountBizEntity>> distributeSafeMultiBizCount(
            Map<CountBiz, Long> bizIncMap, Date occurTime, KeyEntity<? extends KeyType> lock
    ) {
        MongoPersistEntity.PersistEntity persistEntity = new MongoPersistEntity.PersistEntity();

        String occurDate = DateHelper.dateToString(occurTime, DateHelper.DateFormatEnum.fullUntilDay);

        Map<CountBiz, CommonCountTotal> bizCountTotalMap = this.getBizCountTotalMap(bizIncMap.keySet());

        for (Map.Entry<CountBiz, Long> entry : bizIncMap.entrySet()) {
            CommonCountTotal countTotal = bizCountTotalMap.get(entry.getKey());
            if (countTotal != null) {
                continue;
            }

            CommonCountTotal newTotal = CommonCountTotal.of(this.totalDbClazz, entry.getKey().convertToBizDate(occurDate), this.redisAbout, lock);
            bizCountTotalMap.put(entry.getKey(), newTotal);
            persistEntity.getDatabase().insert(newTotal);
        }

        List<CountBizEntity> countBizEntityList = new ArrayList<>();
        for (Map.Entry<CountBiz, CommonCountTotal> entry : bizCountTotalMap.entrySet()) {
            CountBizEntity countBizEntity = new CountBizEntity(entry.getValue(), occurTime, bizIncMap.get(entry.getKey()));

            countBizEntityList.add(countBizEntity);
        }

        MultiCountBizEntity multiCountBizEntity = new MultiCountBizEntity(countBizEntityList);
        Map<KeyEntity<? extends KeyType>, Long> deleteRollbackMap = multiCountBizEntity.collectNeedPersist(persistEntity);
        MongoPersistEntity.CacheInterface deleteRollbackCache = getDeleteRollbackCache(deleteRollbackMap);
        if (deleteRollbackCache != null) {
            persistEntity.getCacheList().add(deleteRollbackCache);
        }

        Map<CountBiz, CountBizEntity> bizCountEntityMap = CollectionHelper.toMap(countBizEntityList, o -> o.biz);

        return Pair.of(persistEntity, bizCountEntityMap);
    }

    @Nullable
    private MongoPersistEntity.CacheInterface getDeleteRollbackCache(Map<KeyEntity<? extends KeyType>, Long> map) {
        if (map.isEmpty()) {
            return null;
        }

        Map<KeyEntity<? extends KeyType>, StrictRedisProvider.AcceptType> rollbackMap = new HashMap<>();
        for (Map.Entry<KeyEntity<? extends KeyType>, Long> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                rollbackMap.put(entry.getKey(), StrictRedisProvider.AcceptType.of(entry.getValue()));
            }
        }

        return new MongoPersistEntity.CacheInterface() {
            @Override
            public void persist() {
                redisAbout.getStrictRedisProvider().delete(map.keySet());
            }

            @Override
            public void rollback() {
                if (!rollbackMap.isEmpty()) {
                    redisAbout.getStrictRedisProvider().multiSet(rollbackMap);
                }
            }
        };
    }

    private class MultiCountBizEntity {
        private final List<CountBizEntity> countBizEntityList;

        MultiCountBizEntity(List<CountBizEntity> countBizEntityList) {
            this.countBizEntityList = countBizEntityList;

            this.finishCacheAbout();
            this.finishCommonCountDateLog();
        }

        public Map<KeyEntity<? extends KeyType>, Long> collectNeedPersist(MongoPersistEntity.PersistEntity persistEntity) {
            Map<KeyEntity<? extends KeyType>, Long> result = new HashMap<>();

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                Map<KeyEntity<? extends KeyType>, Long> tmp = countBizEntity.collectNeedPersist(persistEntity);
                result.putAll(tmp);
            }

            return result;
        }

        private void finishCacheAbout() {
            List<CountBizDate> needReadBeforeLatestCacheTotalBizDates = new ArrayList<>();
            Map<CountBiz, Long> bizBeforeLatestCacheTotalMap = new HashMap<>();

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                CacheStateEnum cacheState = countBizEntity.calCacheState();
                countBizEntity.cacheState = cacheState;

                if (CacheStateEnum.FORWARD.equals(cacheState)) {
                    String beforeLatestCacheDate = countBizEntity.commonCountTotal.getLatestCacheDate();
                    needReadBeforeLatestCacheTotalBizDates.add(countBizEntity.biz.convertToBizDate(beforeLatestCacheDate));
                }
            }

            List<KeyEntity<? extends KeyType>> keyList =
                    CollectionHelper.map(needReadBeforeLatestCacheTotalBizDates, o -> KeyEntity.of(
                            redisAbout.getCountBizDateCacheKeyType(),
                            o.giveJson()
                    ));
            List<Long> cacheList = redisAbout.getStrictRedisProvider().multiGet(keyList, Long.class);

            for (int i = 0; i < needReadBeforeLatestCacheTotalBizDates.size(); i++) {
                CountBizDate bizDate = needReadBeforeLatestCacheTotalBizDates.get(i);
                Long value = cacheList.get(i);
                if (value == null) {
                    value = 0L;
                }

                bizBeforeLatestCacheTotalMap.put(bizDate.extractBiz(), value);
            }

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                countBizEntity.initCacheAbout(bizBeforeLatestCacheTotalMap.get(countBizEntity.biz));
            }
        }

        private void finishCommonCountDateLog() {
            List<CountBizDate> needCommonCountDateLogBizDates = new ArrayList<>();
            Map<CountBiz, CommonCountDateLog> bizCommonCountDateLogMap = new HashMap<>();

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                if (CacheStateEnum.STAY.equals(countBizEntity.cacheState)) {
                    continue;
                }

                if (CacheStateEnum.NO_CACHE.equals(countBizEntity.cacheState)) {
                    needCommonCountDateLogBizDates.add(countBizEntity.bizDate);
                    continue;
                }

                bizCommonCountDateLogMap.put(
                        countBizEntity.bizDate.extractBiz(), CommonCountDateLog.of(dateLogDbClazz, countBizEntity.biz, countBizEntity.beforeLatestCacheDate)
                );
            }

            Map<String, List<CountBizDate>> collectionNameBizDateList = CollectionHelper.groupBy(
                    needCommonCountDateLogBizDates, o -> BaseModel.getAccordCollectionNameByData(mongoTemplate, CommonCountDateLog.splitKeyOf(dateLogDbClazz, o))
            );

            List<CommonCountDateLog> logs = new ArrayList<>();
            for (Map.Entry<String, List<CountBizDate>> entry : collectionNameBizDateList.entrySet()) {
                List<Criteria> orList = new ArrayList<>();
                for (CountBizDate bizDate : entry.getValue()) {
                    orList.add(Criteria.where("t").is(bizDate.getTypeId())
                            .and("b").is(bizDate.getBizType())
                            .and("s").is(bizDate.getSubBizType())
                            .and("d").is(bizDate.getDate())
                    );
                }

                Query query = new Query();
                query.addCriteria(new Criteria().orOperator(orList.toArray(new Criteria[0])));

                logs.addAll(mongoTemplate.find(query, dateLogDbClazz, entry.getKey()));
            }
            Map<CountBizDate, CommonCountDateLog> existBizDateCommonCountDateLogMap = CollectionHelper.toMap(logs, CommonCountDateLog::extractBizDate);

            for (CountBizDate bizDate : needCommonCountDateLogBizDates) {
                CommonCountDateLog exist = existBizDateCommonCountDateLogMap.get(bizDate);
                if (exist == null) {
                    exist = CommonCountDateLog.of(dateLogDbClazz, bizDate);
                }

                bizCommonCountDateLogMap.put(bizDate.extractBiz(), exist);
            }

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                countBizEntity.commonCountDateLog = bizCommonCountDateLogMap.get(countBizEntity.biz);
            }
        }
    }

    public class CountBizEntity {
        private final boolean isDistributeSafe;
        private final CountBizDate bizDate;
        private final CountBiz biz;
        private final long inc;
        private final String currentDate;
        private Boolean needLock = false;

        private CommonCountTotal commonCountTotal;
        private CommonCountDateLog commonCountDateLog;

        private String beforeLatestCacheDate;
        private Long beforeLatestCacheTotal;
        private String nextCacheDate;
        private CacheStateEnum cacheState;

        private long afterAllTotal = 0;
        private long beforeDbTotal = 0;

        private CountBizEntity(CountBizDate bizDate, long inc) {
            this.isDistributeSafe = false;
            this.bizDate = bizDate;
            this.biz = this.bizDate.extractBiz();
            this.inc = inc;
            Date currentTime = new Date();
            this.currentDate = DateHelper.dateToString(currentTime, DateHelper.DateFormatEnum.fullUntilDay);

            if (DateHelper.parseStringDate(this.bizDate.getDate(), DateHelper.DateFormatEnum.fullUntilDay).compareTo(currentTime) > 0) {
                throw new BizException(String.format("bizDate abnormal: %s", this.bizDate.giveJson()));
            }

            this.setCommonCountTotal();

            this.needLock = !Objects.equal(this.bizDate.getDate(), this.commonCountTotal.getLatestCacheDate());
            if (!this.needLock) {
                Date endTime = DateHelper.endOfDay(currentTime);
                Date startTime = DateHelper.startOfDay(currentTime);

                this.needLock = currentTime.after(DateHelper.plusDurationOfDate(endTime, Duration.ofSeconds(-30))) ||
                        currentTime.before(DateHelper.plusDurationOfDate(startTime, Duration.ofSeconds(30)));
            }
            if (!this.needLock) {
                this.needLock = !Objects.equal(this.bizDate.getDate(), this.currentDate);
            }
        }

        private CountBizEntity(CommonCountTotal countTotal, Date occurTime, long inc) {
            this.isDistributeSafe = true;
            Date currentTime = new Date();
            this.currentDate = DateHelper.dateToString(currentTime, DateHelper.DateFormatEnum.fullUntilDay);
            if (occurTime.compareTo(currentTime) > 0) {
                throw new BizException(String.format("occurTime abnormal: %s ", DateHelper.dateToString(occurTime, DateHelper.DateFormatEnum.fullUntilMill)));
            }
            String occurDate = DateHelper.dateToString(occurTime, DateHelper.DateFormatEnum.fullUntilDay);
            this.biz = countTotal.extractBiz();
            this.bizDate = this.biz.convertToBizDate(occurDate);
            this.inc = inc;
            this.commonCountTotal = countTotal;
        }

        //持久化完再调用
        public long giveAfterAllTotal() {
            return this.afterAllTotal;
        }

        private Map<KeyEntity<? extends KeyType>, Long> collectNeedPersist(MongoPersistEntity.PersistEntity persistEntity) {
            Map<KeyEntity<? extends KeyType>, Long> deleteRollbackMap = new HashMap<>();

            this.beforeDbTotal = this.commonCountTotal.getTotal();

            persistEntity.getCacheList().add(this.getIncCache());

            if (CacheStateEnum.STAY.equals(this.cacheState)) {
                return deleteRollbackMap;
            }

            if (this.commonCountDateLog.getId() == null) {
                persistEntity.getDatabase().insert(this.commonCountDateLog);
            }


            if (CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                if (this.inc != 0) {
                    this.commonCountDateLog.setTotal(this.commonCountDateLog.getTotal() + this.inc);
                    this.commonCountTotal.setTotal(this.commonCountTotal.getTotal() + this.inc);

                    persistEntity.getDatabase().save(this.commonCountDateLog);
                    persistEntity.getDatabase().save(this.commonCountTotal);
                    deleteRollbackMap.put(KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), biz.giveJson()), null);
                }

                return deleteRollbackMap;
            }

            if (this.inc == 0) {
                if (!Objects.equal(this.commonCountTotal.getDataIsCold(), true)) {
                    this.commonCountTotal.setDataIsCold(true);
                }
            } else {
                if (Objects.equal(this.commonCountTotal.getDataIsCold(), true)) {
                    this.commonCountTotal.setDataIsCold(false);
                }
            }

            this.commonCountTotal.setLatestCacheDate(this.nextCacheDate);
            this.commonCountTotal.setTotal(this.commonCountTotal.getTotal() + this.beforeLatestCacheTotal);
            this.commonCountDateLog.setTotal(this.beforeLatestCacheTotal);

            persistEntity.getDatabase().save(this.commonCountTotal);
            deleteRollbackMap.put(
                    KeyEntity.of(
                            redisAbout.getCountBizDateCacheKeyType(),
                            biz.convertToBizDate(beforeLatestCacheDate).giveJson()
                    ),
                    this.beforeLatestCacheTotal
            );
            deleteRollbackMap.put(KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), biz.giveJson()), null);

            return deleteRollbackMap;
        }

        private MongoPersistEntity.CacheInterface getIncCache() {
            return new MongoPersistEntity.CacheInterface() {
                @Override
                public void persist() {
                    afterAllTotal = incAndCacheAfterAllLua();
                }

                @Override
                public void rollback() {
                    KeyEntity<? extends KeyType> keyEntity = KeyEntity.of(
                            redisAbout.getCountBizDateCacheKeyType(),
                            biz.convertToBizDate(nextCacheDate).giveJson()
                    );

                    long nextInc = getNextCacheInc();
                    if (nextInc != 0) {
                        redisAbout.getStrictRedisProvider().incrBy(keyEntity, nextInc * -1);

                        afterAllTotal -= nextInc;
                    }

                    redisAbout.getStrictRedisProvider().delete(
                            KeyEntity.of(redisAbout.getCountBizAfterAllTotalCacheKeyType(), biz.giveJson())
                    );
                }
            };
        }

        private Long incAndCacheAfterAllLua() {
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(COMMON_COUNT_LUA, Long.class);

            KeyEntity<? extends KeyType> beforeKeyEntity = KeyEntity.of(
                    redisAbout.getCountBizDateCacheKeyType(),
                    biz.convertToBizDate(beforeLatestCacheDate).giveJson()
            );

            KeyEntity<? extends KeyType> nextKeyEntity = KeyEntity.of(
                    redisAbout.getCountBizDateCacheKeyType(),
                    biz.convertToBizDate(nextCacheDate).giveJson()
            );

            KeyEntity<? extends KeyType> afterAllKeyEntity = KeyEntity.of(
                    redisAbout.getCountBizAfterAllTotalCacheKeyType(),
                    biz.giveJson()
            );

            Object[] values = new Long[]{this.getNextCacheInc(), this.getDbTotal(), redisAbout.getAfterAllTotalCacheDuration().toSeconds()};

            return redisAbout.getStrictRedisProvider().executeLua(redisScript, Lists.newArrayList(beforeKeyEntity, nextKeyEntity, afterAllKeyEntity), values);
        }

        private long getDbTotal() {
            if (!CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                return this.beforeDbTotal;
            }

            return this.beforeDbTotal + this.inc;
        }

        private long getNextCacheInc() {
            if (CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                return 0;
            }

            return this.inc;
        }

        private void setCommonCountTotal() {
            if (this.isDistributeSafe) {
                throw new BizException("distributeSafe cant use this func");
            }

            CommonCountTotal cache = redisAbout.getStrictRedisProvider().get(
                    KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), this.biz.giveJson()),
                    CommonCountTotal.class
            );
            if (cache != null && this.currentDate.equals(cache.getLatestCacheDate()) && this.currentDate.equals(this.bizDate.getDate())) {
                this.commonCountTotal = cache;
                return;
            }

            String collectionName = BaseModel.getAccordCollectionNameByData(mongoTemplate, CommonCountTotal.splitKeyOf(totalDbClazz, this.biz));

            DefaultResourceInterface<CommonCountTotal> entity = new DefaultResourceInterface<>() {
                @Override
                public KeyEntity<? extends KeyType> getLock() {
                    return KeyEntity.of(
                            redisAbout.getCommonCountTotalCreateLockType(),
                            biz.giveJson()
                    );
                }

                @Override
                public CommonCountTotal findExist() {
                    Query query = new Query();

                    query.addCriteria(Criteria.where("t").is(biz.getTypeId()));
                    query.addCriteria(Criteria.where("b").is(biz.getBizType()));
                    query.addCriteria(Criteria.where("s").is(biz.getSubBizType()));

                    return mongoTemplate.findOne(query, totalDbClazz, collectionName);
                }

                @Override
                public CommonCountTotal createWhenNotExist() {
                    CommonCountTotal data = CommonCountTotal.of(totalDbClazz, bizDate, redisAbout);

                    return mongoTemplate.insert(data, collectionName);
                }
            };

            this.commonCountTotal = entity.getDefault(redisAbout.getStrictRedisProvider(), o -> {
                boolean needUpdate = o.checkNeedUpdateCache(cache);

                if (needUpdate) {
                    redisAbout.getStrictRedisProvider().set(
                            KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), this.biz.giveJson()),
                            o.toRedisAccept(),
                            Duration.ofHours(1)
                    );
                }

                return null;
            });
        }

        private void initCacheAbout(Long beforeLatestCacheTotal) {
            if (!this.isDistributeSafe) {
                throw new BizException("only for distributeSafe");
            }

            this.beforeLatestCacheDate = this.commonCountTotal.getLatestCacheDate();

            //代表日志是上次缓存日期之前
            if (CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                this.nextCacheDate = this.commonCountTotal.getLatestCacheDate();
                return;
            }

            this.nextCacheDate = this.bizDate.getDate();
            //代表日志的日期等于上次缓存日期
            if (CacheStateEnum.STAY.equals(this.cacheState)) {
                return;
            }

            //代表日志在上次缓存日期之后
            this.beforeLatestCacheTotal = beforeLatestCacheTotal;
        }

        private CacheStateEnum calCacheState() {
            CacheStateEnum cacheState;

            int compareResult = DateHelper.parseStringDate(this.commonCountTotal.getLatestCacheDate(), DateHelper.DateFormatEnum.fullUntilDay).
                    compareTo(DateHelper.parseStringDate(this.bizDate.getDate(), DateHelper.DateFormatEnum.fullUntilDay));

            if (compareResult < 0) {
                cacheState = CacheStateEnum.FORWARD;
            } else if (compareResult == 0) {
                cacheState = CacheStateEnum.STAY;
            } else {
                cacheState = CacheStateEnum.NO_CACHE;
            }

            return cacheState;
        }

        private void initCacheAbout() {
            if (this.isDistributeSafe) {
                throw new BizException("distributeSafe cant use this func");
            }

            this.cacheState = this.calCacheState();

            this.beforeLatestCacheDate = this.commonCountTotal.getLatestCacheDate();

            //代表日志是上次缓存日期之前
            if (CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                this.nextCacheDate = this.commonCountTotal.getLatestCacheDate();
                return;
            }

            this.nextCacheDate = this.bizDate.getDate();
            //代表日志的日期等于上次缓存日期
            if (CacheStateEnum.STAY.equals(this.cacheState)) {
                return;
            }

            //代表日志在上次缓存日期之后
            Long value = redisAbout.getStrictRedisProvider().get(
                    KeyEntity.of(
                            redisAbout.getCountBizDateCacheKeyType(),
                            this.biz.convertToBizDate(this.beforeLatestCacheDate).giveJson()
                    ),
                    Long.class
            );
            if (value == null) {
                value = 0L;
            }

            this.beforeLatestCacheTotal = value;
        }

        private void setCommonCountDateLog() {
            if (this.isDistributeSafe) {
                throw new BizException("distributeSafe cant use this func");
            }

            if (CacheStateEnum.STAY.equals(this.cacheState)) {
                return;
            }

            if (CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                String collectionName = BaseModel.getAccordCollectionNameByData(mongoTemplate, CommonCountDateLog.splitKeyOf(dateLogDbClazz, this.bizDate));

                Query query = new Query();

                query.addCriteria(Criteria.where("t").is(this.bizDate.getTypeId()));
                query.addCriteria(Criteria.where("b").is(this.bizDate.getBizType()));
                query.addCriteria(Criteria.where("s").is(this.bizDate.getSubBizType()));
                query.addCriteria(Criteria.where("d").is(this.bizDate.getDate()));

                CommonCountDateLog exist = mongoTemplate.findOne(query, dateLogDbClazz, collectionName);
                if (exist == null) {
                    exist = CommonCountDateLog.of(dateLogDbClazz, this.bizDate);
                }

                this.commonCountDateLog = exist;
                return;
            }

            this.commonCountDateLog = CommonCountDateLog.of(dateLogDbClazz, this.biz, this.beforeLatestCacheDate);
        }
    }


    private enum CacheStateEnum {
        NO_CACHE, STAY, FORWARD
    }

    @Setter
    @Getter
    @JsonPropertyOrder({"t", "b", "s", "d"})
    public static class CountBizDate {
        @JsonProperty("t")
        private String typeId;

        @JsonProperty("b")
        private String bizType;

        @JsonProperty("s")
        private String subBizType;

        @JsonProperty("d")
        private String date;

        @JsonIgnore
        private CountBiz biz;

        @JsonIgnore
        private String json;

        public CountBiz extractBiz() {
            if (this.biz != null) {
                return this.biz;
            }

            CountBiz data = new CountBiz();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);

            this.biz = data;

            return data;
        }

        public String giveJson() {
            if (this.json != null) {
                return this.json;
            }

            String data = JsonHelper.writeValueAsString(this);
            this.json = data;

            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountBizDate that = (CountBizDate) o;
            return Objects.equal(typeId, that.typeId) && Objects.equal(bizType, that.bizType) &&
                    Objects.equal(subBizType, that.subBizType) && Objects.equal(date, that.date);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(typeId, bizType, subBizType, date);
        }
    }


    @Setter
    @Getter
    @JsonPropertyOrder({"t", "b", "s"})
    public static class CountBiz {
        @JsonProperty("t")
        private String typeId;

        @JsonProperty("b")
        private String bizType;

        @JsonProperty("s")
        private String subBizType;

        @JsonIgnore
        private Map<String, CountBizDate> dateMap = new HashMap<>();

        @JsonIgnore
        private String json;

        public CountBizDate convertToBizDate(String date) {
            if (this.dateMap.containsKey(date)) {
                return this.dateMap.get(date);
            }

            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(date);

            this.dateMap.put(date, data);

            return data;
        }

        public String giveJson() {
            if (this.json != null) {
                return this.json;
            }

            String data = JsonHelper.writeValueAsString(this);
            this.json = data;

            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountBiz countBiz = (CountBiz) o;
            return Objects.equal(typeId, countBiz.typeId) && Objects.equal(bizType, countBiz.bizType) && Objects.equal(subBizType, countBiz.subBizType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(typeId, bizType, subBizType);
        }
    }


    @Setter
    @Getter
    @CompoundIndexes({
            @CompoundIndex(name = "typeId_bizType_subBizType", def = "{'t':1, 'b':1, 's': 1}", unique = true),
            @CompoundIndex(name = "scope_latestCacheStamp_dataIsCold", def = "{'sc':1, 'st':1, 'c':1}")
    })
    public static class CommonCountTotal extends BaseModel implements ISplitCollection {
        @Field(value = "t")
        private String typeId;

        @Field(value = "b")
        private String bizType;

        @Field(value = "s")
        private String subBizType;

        private Long total = 0L;

        @Field(value = "d")
        private String latestCacheDate;

        @Field(value = "st")
        private Long latestCacheStamp;

        @Field(value = "c")
        private Boolean dataIsCold = false;

        @Field(value = "sc")
        private String scope;

        private DistributedKeyProvider.KeyFinder lockFinder;

        private String lockId;

        @JsonIgnore
        @Transient
        private CountBiz biz;

        public void setLatestCacheDate(String date) {
            this.latestCacheDate = date;
            this.latestCacheStamp = DateHelper.parseStringDate(date, DateHelper.DateFormatEnum.fullUntilDay).getTime();
        }

        @Override
        public String calSplitIndex() {
            int offset = Math.abs(this.typeId.hashCode() % 2);

            return String.valueOf(offset);
        }

        public CountBiz extractBiz() {
            if (this.biz != null) {
                return this.biz;
            }

            CountBiz data = new CountBiz();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);

            this.biz = data;

            return data;
        }

        public StrictRedisProvider.AcceptType toRedisAccept() {
            return StrictRedisProvider.AcceptType.of(JsonHelper.writeValueAsString(this));
        }

        public boolean checkNeedUpdateCache(@Nullable CommonCountTotal cache) {
            if (cache != null && !this.extractBiz().equals(cache.extractBiz())) {
                throw new BizException("two diff biz");
            }

            boolean needUpdate = false;

            if (cache == null) {
                needUpdate = true;
            } else if (!Objects.equal(cache.getLatestCacheDate(), this.latestCacheDate)) {
                needUpdate = true;
            } else if (!Objects.equal(cache.getTotal(), this.total)) {
                needUpdate = true;
            } else if (!Objects.equal(cache.getDataIsCold(), this.dataIsCold)) {
                needUpdate = true;
            }

            return needUpdate;
        }

        public static <M extends CommonCountTotal> M splitKeyOf(Class<M> clazz, CountBiz biz) {
            M data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
                data.setTypeId(biz.getTypeId());
                data.setBizType(biz.getBizType());
                data.setSubBizType(biz.getSubBizType());
            } catch (Exception e) {
                throw new BizException(String.format("createTotal fail: %s", e.getMessage()));
            }

            return data;
        }

        public static <M extends CommonCountTotal> M of(Class<M> clazz, CountBizDate bizDate, RedisAbout<?> redisAbout) {
            M data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
                data.setTypeId(bizDate.getTypeId());
                data.setBizType(bizDate.getBizType());
                data.setSubBizType(bizDate.getSubBizType());
                data.setTotal(0L);
                data.setLatestCacheDate(bizDate.getDate());
                data.setDataIsCold(false);
                data.setScope(redisAbout.getStrictRedisProvider().getDistributedKeyProvider().getScope());
            } catch (Exception e) {
                throw new BizException(String.format("createTotal fail: %s", e.getMessage()));
            }

            return data;
        }

        public static <M extends CommonCountTotal> M of(Class<M> clazz, CountBizDate bizDate, RedisAbout<?> redisAbout, KeyEntity<? extends KeyType> lock) {
            M data = CommonCountTotal.of(clazz, bizDate, redisAbout);

            data.setLockFinder(DistributedKeyProvider.KeyFinder.of(lock.getType()));
            data.setLockId(lock.getTypeId());

            return data;
        }
    }

    @Setter
    @Getter
    @CompoundIndexes({
            @CompoundIndex(name = "typeId_bizType_subBizType_date", def = "{'t':1, 'b':1, 's': 1, 'd': 1}", unique = true)
    })
    public static class CommonCountDateLog extends BaseModel implements ISplitCollection {
        @Field(value = "t")
        private String typeId;

        @Field(value = "b")
        private String bizType;

        @Field(value = "s")
        private String subBizType;

        @Field(value = "d")
        private String date;

        private Long total = 0L;

        @JsonIgnore
        @Transient
        private CountBiz biz;

        @JsonIgnore
        @Transient
        private CountBizDate bizDate;

        @Override
        public String calSplitIndex() {
            Date date = DateHelper.parseStringDate(this.date, DateHelper.DateFormatEnum.fullUntilDay);

            return DateHelper.dateToString(date, DateHelper.DateFormatEnum.fullUntilMonth);
        }

        public CountBiz extractBiz() {
            if (this.biz != null) {
                return this.biz;
            }

            CountBiz data = new CountBiz();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);

            this.biz = data;

            return data;
        }

        public CountBizDate extractBizDate() {
            if (this.bizDate != null) {
                return this.bizDate;
            }

            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(this.date);

            this.bizDate = data;

            return data;
        }

        public static <M extends CommonCountDateLog> M splitKeyOf(Class<M> clazz, CountBizDate bizDate) {
            M data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
                data.setTypeId(bizDate.getTypeId());
                data.setBizType(bizDate.getBizType());
                data.setSubBizType(bizDate.getSubBizType());
                data.setDate(bizDate.getDate());
            } catch (Exception e) {
                throw new BizException(String.format("createDateLog fail: %s", e.getMessage()));
            }

            return data;
        }

        public static <M extends CommonCountDateLog> M of(Class<M> clazz, CountBizDate bizDate) {
            M data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
                data.setTypeId(bizDate.getTypeId());
                data.setBizType(bizDate.getBizType());
                data.setSubBizType(bizDate.getSubBizType());
                data.setDate(bizDate.getDate());
                data.setTotal(0L);
            } catch (Exception e) {
                throw new BizException(String.format("createDateLog fail: %s", e.getMessage()));
            }

            return data;
        }

        public static <M extends CommonCountDateLog> M of(Class<M> clazz, CountBiz biz, String date) {
            M data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
                data.setTypeId(biz.getTypeId());
                data.setBizType(biz.getBizType());
                data.setSubBizType(biz.getSubBizType());
                data.setDate(date);
                data.setTotal(0L);
            } catch (Exception e) {
                throw new BizException(String.format("createDateLog fail: %s", e.getMessage()));
            }

            return data;
        }
    }

    @Setter
    @Getter
    public static class CommonCountPersistHistory {
        private String typeId;

        private String bizType;

        private String subBizType;

        private Long total = 0L;

        private String date;

        @JsonIgnore
        private CountBizDate bizDate;

        public static CommonCountPersistHistory of(CountBizDate bizDate, long inc) {
            CommonCountPersistHistory data = new CommonCountPersistHistory();

            data.setTypeId(bizDate.getTypeId());
            data.setBizType(bizDate.getBizType());
            data.setSubBizType(bizDate.getSubBizType());
            data.setDate(bizDate.getDate());
            data.setTotal(inc);

            return data;
        }

        public CountBizDate extractBizDate() {
            if (this.bizDate != null) {
                return this.bizDate;
            }

            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(this.date);

            this.bizDate = data;

            return data;
        }

        public StrictRedisProvider.AcceptType toRedisAccept() {
            return StrictRedisProvider.AcceptType.of(JsonHelper.writeValueAsString(this));
        }
    }


    @Setter
    @Getter
    public static class RedisAbout<M extends KeyType> {
        private StrictRedisProvider strictRedisProvider;

        private Duration afterAllTotalCacheDuration;

        private M commonCountTotalCacheKeyType;

        private M commonCountTotalCreateLockType;

        private M countBizDateCacheKeyType;

        private M commonCountPersistHistoryLockType;

        private M countBizAfterAllTotalCacheKeyType;

        private KeyEntity<M> countHistoryHashKey;

        public static <M extends KeyType> RedisAbout<M> of(
                StrictRedisProvider strictRedisProvider, Duration afterAllTotalCacheDuration,
                M commonCountTotalCacheKeyType, M commonCountTotalCreateLockType, M countBizDateCacheKeyType,
                M commonCountPersistHistoryLockType, M countBizAfterAllTotalCacheKeyType, KeyEntity<M> countHistoryHashKey
        ) {
            RedisAbout<M> data = new RedisAbout<>();

            data.setStrictRedisProvider(strictRedisProvider);
            data.setAfterAllTotalCacheDuration(afterAllTotalCacheDuration);
            data.setCommonCountTotalCacheKeyType(commonCountTotalCacheKeyType);
            data.setCommonCountTotalCreateLockType(commonCountTotalCreateLockType);
            data.setCountBizDateCacheKeyType(countBizDateCacheKeyType);
            data.setCommonCountPersistHistoryLockType(commonCountPersistHistoryLockType);
            data.setCountBizAfterAllTotalCacheKeyType(countBizAfterAllTotalCacheKeyType);
            data.setCountHistoryHashKey(countHistoryHashKey);

            return data;
        }
    }

    @Setter
    @Getter
    public static class CountLock {
        private DistributedKeyProvider.KeyFinder lockFinder;

        private String lockId;

        public static CountLock of(CommonCountTotal total) {
            CountLock countLock = new CountLock();

            countLock.setLockFinder(total.getLockFinder());
            countLock.setLockId(total.getLockId());

            return countLock;
        }

        public static Map<CountLock, List<CommonCountTotal>> group(List<CommonCountTotal> totals) {
            return CollectionHelper.groupBy(totals, CountLock::of);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountLock countLock = (CountLock) o;
            return java.util.Objects.equals(lockFinder, countLock.lockFinder) && java.util.Objects.equals(lockId, countLock.lockId);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(lockFinder, lockId);
        }
    }
}
