package io.github.chaogeoop.base.business.common;

import io.github.chaogeoop.base.business.common.interfaces.CheckResourceValidToHandleInterface;
import io.github.chaogeoop.base.business.common.interfaces.DefaultResourceInterface;
import io.github.chaogeoop.base.business.mongodb.*;
import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.redis.DistributedKeyType;
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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

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
    private final RedisAbout<? extends DistributedKeyType> redisAbout;
    private final Function<List<String>, NullType> countHistorySender;
    private final Class<? extends CommonCountTotal> totalDbClazz;
    private final Class<? extends CommonCountDateLog> dateLogDbClazz;


    public CommonCountProvider(
            PersistProvider persistProvider,
            RedisAbout<? extends DistributedKeyType> redisAbout,
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

    public Map<CountBiz, Long> getBizTotalMapWithCacheExcept(Set<CountBiz> allBizList, Set<CountBiz> skipCacheBizList) {
        if (!this.redisAbout.cacheAfterAllTotal) {
            return this.getBizTotalMapWithoutCache(allBizList);
        }

        for (CountBiz skipCacheBiz : skipCacheBizList) {
            if (!allBizList.contains(skipCacheBiz)) {
                throw new BizException(String.format("skipList contains allList not contain: %s", skipCacheBiz));
            }
        }

        List<CountBiz> list = Lists.newArrayList(allBizList);
        list.removeIf(skipCacheBizList::contains);

        return this.redisAbout.getRedisProvider().getMapFromValueCache(
                this.redisAbout.getCountBizAfterAllTotalCacheKeyType(),
                this.redisAbout.getCacheDuration(),
                Long.class,
                list,
                needCacheList -> {
                    Set<CountBiz> toCacheList = Sets.newHashSet(needCacheList);
                    toCacheList.addAll(skipCacheBizList);
                    return getBizTotalMapWithoutCache(toCacheList);
                }
        );
    }

    public Map<CountBiz, Long> getBizTotalMap(Set<CountBiz> bizList) {
        if (!this.redisAbout.cacheAfterAllTotal) {
            return this.getBizTotalMapWithoutCache(bizList);
        }

        List<CountBiz> list = Lists.newArrayList(bizList);

        return this.redisAbout.getRedisProvider().getMapFromValueCache(
                this.redisAbout.getCountBizAfterAllTotalCacheKeyType(),
                this.redisAbout.getCacheDuration(),
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

        List<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>> latestCacheKeys = CollectionHelper.map(
                hasTotalBizDateList,
                o -> DistributedKeyProvider.KeyEntity.of(redisAbout.getCountBizDateCacheKeyType(), JsonHelper.writeValueAsString(o))
        );

        List<Long> cacheBizDateTotals = redisAbout.getRedisProvider().multiGet(latestCacheKeys, Long.class);

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
            List<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>> needReadCacheKeys = CollectionHelper.map(
                    needReadCacheBizDateList,
                    o -> DistributedKeyProvider.KeyEntity.of(redisAbout.getCountBizDateCacheKeyType(), JsonHelper.writeValueAsString(o))
            );
            List<Long> values = redisAbout.getRedisProvider().multiGet(needReadCacheKeys, Long.class);
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
                query.addCriteria(Criteria.where("ds").is(false));
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

                Map<CountBizDate, Long> bizDateIncMap = new HashMap<>();
                for (CommonCountTotal log : logs) {
                    bizDateIncMap.put(log.convertToBizDate(currentDate), 0L);
                }

                MongoPersistEntity.PersistEntity persistEntity = this.insertPersistHistory(bizDateIncMap);
                this.persistProvider.persist(Lists.newArrayList(persistEntity));
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

        Map<String, RedisProvider.AcceptType> map = new HashMap<>();
        for (Map<CountBizDate, Long> bizDateIncMap : bizDateIncMapList) {
            for (Map.Entry<CountBizDate, Long> entry : bizDateIncMap.entrySet()) {
                CommonCountPersistHistory data = CommonCountPersistHistory.of(entry.getKey(), entry.getValue());
                String key = UUID.randomUUID().toString();

                map.put(key, RedisProvider.AcceptType.of(JsonHelper.writeValueAsString(data)));
            }
        }

        MongoPersistEntity.CacheInterface historyStoreCache = new MongoPersistEntity.CacheInterface() {
            @Override
            public void persist() {
                redisAbout.getRedisProvider().hmset(redisAbout.getCountHistoryHashKey(), map);
            }

            @Override
            public void rollback() {
                redisAbout.getRedisProvider().hdel(redisAbout.getCountHistoryHashKey(), map.keySet());
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
            public DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> getLock() {
                return DistributedKeyProvider.KeyEntity.of(
                        redisAbout.getCommonCountPersistHistoryLockType(),
                        id
                );
            }

            @Override
            public CommonCountPersistHistory findResource() {
                return redisAbout.getRedisProvider().hget(redisAbout.getCountHistoryHashKey(), id, CommonCountPersistHistory.class);
            }

            @Override
            public boolean validToHandle(CommonCountPersistHistory resource) {
                return true;
            }
        };

        entity.handle(this.redisAbout.getRedisProvider(), o -> {
            CountBizDate bizDate = o.extractBizDate();
            CountBiz biz = bizDate.extractBiz();

            CountBizEntity countBizEntity = new CountBizEntity(bizDate, o.getTotal());
            MongoPersistEntity.PersistEntity persistEntity = new MongoPersistEntity.PersistEntity();


            MongoPersistEntity.CacheInterface deleteHistoryCache = new MongoPersistEntity.CacheInterface() {
                @Override
                public void persist() {
                    redisAbout.getRedisProvider().hdel(redisAbout.getCountHistoryHashKey(), Sets.newHashSet(id));
                }

                @Override
                public void rollback() {

                }
            };

            if (!countBizEntity.needLock) {
                countBizEntity.initCacheAbout();
                countBizEntity.setCommonCountDateLog();
                countBizEntity.collectNeedPersist(persistEntity);

                persistEntity.getCacheList().add(deleteHistoryCache);

                this.persistProvider.persist(Lists.newArrayList(persistEntity));
            } else {
                this.redisAbout.getRedisProvider().exeFuncWithLock(
                        DistributedKeyProvider.KeyEntity.of(this.redisAbout.getCommonCountTotalCreateLockType(), JsonHelper.writeValueAsString(biz)),
                        M -> {
                            countBizEntity.setCommonCountTotal();
                            countBizEntity.initCacheAbout();
                            countBizEntity.setCommonCountDateLog();
                            countBizEntity.collectNeedPersist(persistEntity);

                            persistEntity.getCacheList().add(deleteHistoryCache);

                            this.persistProvider.persist(Lists.newArrayList(persistEntity));

                            return null;
                        }
                );
            }

            if (this.redisAbout.cacheAfterAllTotal) {
                long afterAllTotal = countBizEntity.afterAllTotal;

                this.redisAbout.getRedisProvider().set(
                        DistributedKeyProvider.KeyEntity.of(this.redisAbout.getCountBizAfterAllTotalCacheKeyType(), JsonHelper.writeValueAsString(biz)),
                        RedisProvider.AcceptType.of(afterAllTotal),
                        this.redisAbout.getCacheDuration()
                );
            }

            return null;
        }, o -> null);
    }


    public Pair<MongoPersistEntity.PersistEntity, Map<CountBiz, CountBizEntity>> distributeSafeMultiBizCount(Map<CountBiz, Long> bizIncMap, Date occurTime) {
        MongoPersistEntity.PersistEntity persistEntity = new MongoPersistEntity.PersistEntity();

        String occurDate = DateHelper.dateToString(occurTime, DateHelper.DateFormatEnum.fullUntilDay);

        Map<CountBiz, CommonCountTotal> bizCountTotalMap = this.getBizCountTotalMap(bizIncMap.keySet());

        for (Map.Entry<CountBiz, Long> entry : bizIncMap.entrySet()) {
            CommonCountTotal countTotal = bizCountTotalMap.get(entry.getKey());
            if (countTotal != null) {
                continue;
            }

            CommonCountTotal newTotal = CommonCountTotal.of(totalDbClazz, entry.getKey().convertToBizDate(occurDate));
            bizCountTotalMap.put(entry.getKey(), newTotal);
            persistEntity.getDatabase().insert(newTotal);
        }

        List<CountBizEntity> countBizEntityList = new ArrayList<>();
        for (Map.Entry<CountBiz, CommonCountTotal> entry : bizCountTotalMap.entrySet()) {
            CountBizEntity countBizEntity = new CountBizEntity(entry.getValue(), occurTime, bizIncMap.get(entry.getKey()));

            countBizEntityList.add(countBizEntity);
        }

        MultiCountBizEntity multiCountBizEntity = new MultiCountBizEntity(countBizEntityList);
        multiCountBizEntity.collectNeedPersist(persistEntity);

        Map<CountBiz, CountBizEntity> bizCountEntityMap = CollectionHelper.toMap(countBizEntityList, o -> o.biz);

        Pair<MongoPersistEntity.PersistEntity, Map<CountBiz, CountBizEntity>> result = Pair.of(persistEntity, bizCountEntityMap);

        if (!this.redisAbout.cacheAfterAllTotal) {
            return result;
        }

        for (CountBizEntity countBizEntity : countBizEntityList) {
            DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity =
                    DistributedKeyProvider.KeyEntity.of(
                            redisAbout.getCountBizAfterAllTotalCacheKeyType(),
                            JsonHelper.writeValueAsString(countBizEntity.biz)
                    );

            MongoPersistEntity.CacheInterface cache = new MongoPersistEntity.CacheInterface() {

                @Override
                public void persist() {
                    long afterAllTotal = countBizEntity.afterAllTotal;

                    redisAbout.getRedisProvider().set(
                            keyEntity,
                            RedisProvider.AcceptType.of(afterAllTotal),
                            redisAbout.getCacheDuration()
                    );
                }

                @Override
                public void rollback() {
                    redisAbout.getRedisProvider().delete(keyEntity);
                }
            };
            persistEntity.getCacheList().add(cache);
        }

        return result;
    }


    private class MultiCountBizEntity {
        private final List<CountBizEntity> countBizEntityList;

        MultiCountBizEntity(List<CountBizEntity> countBizEntityList) {
            this.countBizEntityList = countBizEntityList;

            this.finishCacheAbout();
            this.finishCommonCountDateLog();
        }

        public void collectNeedPersist(MongoPersistEntity.PersistEntity persistEntity) {
            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                countBizEntity.collectNeedPersist(persistEntity);
            }
        }

        private void finishCacheAbout() {
            List<CountBizDate> needReadBeforeLatestCacheTotalBizDates = new ArrayList<>();
            Map<CountBizDate, Long> bizDateBeforeLatestCacheTotalMap = new HashMap<>();

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                CacheStateEnum cacheState = countBizEntity.calCacheState();
                countBizEntity.cacheState = cacheState;

                if (!CacheStateEnum.STAY.equals(cacheState)) {
                    String beforeLatestCacheDate = countBizEntity.commonCountTotal.getLatestCacheDate();
                    needReadBeforeLatestCacheTotalBizDates.add(countBizEntity.biz.convertToBizDate(beforeLatestCacheDate));
                }
            }

            List<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>> keyList =
                    CollectionHelper.map(needReadBeforeLatestCacheTotalBizDates, o -> DistributedKeyProvider.KeyEntity.of(
                            redisAbout.getCountBizDateCacheKeyType(),
                            JsonHelper.writeValueAsString(o)
                    ));
            List<Long> cacheList = redisAbout.getRedisProvider().multiGet(keyList, Long.class);

            for (int i = 0; i < needReadBeforeLatestCacheTotalBizDates.size(); i++) {
                CountBizDate bizDate = needReadBeforeLatestCacheTotalBizDates.get(i);
                Long value = cacheList.get(i);
                if (value == null) {
                    value = 0L;
                }

                bizDateBeforeLatestCacheTotalMap.put(bizDate, value);
            }

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                countBizEntity.initCacheAbout(bizDateBeforeLatestCacheTotalMap.get(countBizEntity.bizDate));
            }
        }

        private void finishCommonCountDateLog() {
            List<CountBizDate> needCommonCountDateLogBizDates = new ArrayList<>();
            Map<CountBizDate, CommonCountDateLog> bizDateCommonCountDateLogMap = new HashMap<>();

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                if (CacheStateEnum.STAY.equals(countBizEntity.cacheState)) {
                    continue;
                }

                if (CacheStateEnum.NO_CACHE.equals(countBizEntity.cacheState)) {
                    needCommonCountDateLogBizDates.add(countBizEntity.bizDate);
                    continue;
                }

                bizDateCommonCountDateLogMap.put(
                        countBizEntity.bizDate, CommonCountDateLog.of(dateLogDbClazz, countBizEntity.biz, countBizEntity.beforeLatestCacheDate)
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
                if (exist != null) {
                    exist = CommonCountDateLog.of(dateLogDbClazz, bizDate);
                }

                bizDateCommonCountDateLogMap.put(bizDate, exist);
            }

            for (CountBizEntity countBizEntity : this.countBizEntityList) {
                countBizEntity.commonCountDateLog = bizDateCommonCountDateLogMap.get(countBizEntity.bizDate);
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

        private CountBizEntity(CountBizDate bizDate, long inc) {
            this.isDistributeSafe = false;
            this.bizDate = bizDate;
            this.biz = this.bizDate.extractBiz();
            this.inc = inc;
            Date currentTime = new Date();
            this.currentDate = DateHelper.dateToString(currentTime, DateHelper.DateFormatEnum.fullUntilDay);

            if (DateHelper.parseStringDate(this.bizDate.getDate(), DateHelper.DateFormatEnum.fullUntilDay).compareTo(currentTime) > 0) {
                throw new BizException(String.format("bizDate abnormal: %s", JsonHelper.writeValueAsString(this.bizDate)));
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

        private void collectNeedPersist(MongoPersistEntity.PersistEntity persistEntity) {
            this.afterAllTotal = this.commonCountTotal.getTotal();

            if (CacheStateEnum.STAY.equals(this.cacheState)) {
                persistEntity.getCacheList().add(this.getIncCache());
                return;
            }

            if (this.commonCountDateLog.getId() == null) {
                persistEntity.getDatabase().insert(this.commonCountDateLog);
            }

            this.afterAllTotal += this.beforeLatestCacheTotal;

            if (CacheStateEnum.NO_CACHE.equals(this.cacheState)) {
                if (this.inc != 0) {
                    this.commonCountDateLog.setTotal(this.commonCountDateLog.getTotal() + this.inc);
                    this.commonCountTotal.setTotal(this.commonCountTotal.getTotal() + this.inc);
                    this.afterAllTotal += this.inc;

                    persistEntity.getDatabase().save(this.commonCountDateLog);
                    persistEntity.getDatabase().save(this.commonCountTotal);
                    persistEntity.getCacheList().add(this.getDeleteCommonCountTotalCache());
                }

                return;
            }

            if (this.inc == 0) {
                if (!Objects.equal(this.commonCountTotal.getDataIsCold(), true)) {
                    this.commonCountTotal.setDataIsCold(true);
                }
            } else {
                if (Objects.equal(this.commonCountTotal.getDataIsCold(), true)) {
                    this.commonCountTotal.setDataIsCold(false);
                }
                persistEntity.getCacheList().add(this.getIncCache());
            }

            this.commonCountTotal.setLatestCacheDate(this.nextCacheDate);
            this.commonCountTotal.setTotal(this.commonCountTotal.getTotal() + this.beforeLatestCacheTotal);
            this.commonCountDateLog.setTotal(this.beforeLatestCacheTotal);

            persistEntity.getDatabase().save(this.commonCountTotal);
            persistEntity.getCacheList().add(this.getDeleteDateCountCache());
            persistEntity.getCacheList().add(this.getDeleteCommonCountTotalCache());
        }

        private MongoPersistEntity.CacheInterface getDeleteCommonCountTotalCache() {
            return new MongoPersistEntity.CacheInterface() {
                @Override
                public void persist() {
                    redisAbout.getRedisProvider().delete(
                            DistributedKeyProvider.KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), JsonHelper.writeValueAsString(biz))
                    );
                }

                @Override
                public void rollback() {

                }
            };
        }

        private MongoPersistEntity.CacheInterface getIncCache() {
            return new MongoPersistEntity.CacheInterface() {
                private final DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity = DistributedKeyProvider.KeyEntity.of(
                        redisAbout.getCountBizDateCacheKeyType(),
                        JsonHelper.writeValueAsString(biz.convertToBizDate(nextCacheDate))
                );

                @Override
                public void persist() {
                    long cacheTotal = redisAbout.getRedisProvider().incrBy(keyEntity, inc);

                    afterAllTotal += cacheTotal;
                }

                @Override
                public void rollback() {
                    redisAbout.getRedisProvider().incrBy(keyEntity, inc * -1);

                    afterAllTotal -= inc;
                }
            };
        }

        private MongoPersistEntity.CacheInterface getDeleteDateCountCache() {
            return new MongoPersistEntity.CacheInterface() {
                private final DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity = DistributedKeyProvider.KeyEntity.of(
                        redisAbout.getCountBizDateCacheKeyType(),
                        JsonHelper.writeValueAsString(biz.convertToBizDate(beforeLatestCacheDate))
                );

                @Override
                public void persist() {
                    redisAbout.getRedisProvider().delete(keyEntity);
                }

                @Override
                public void rollback() {
                    redisAbout.getRedisProvider().set(keyEntity, RedisProvider.AcceptType.of(beforeLatestCacheTotal));
                }
            };
        }

        private void setCommonCountTotal() {
            if (this.isDistributeSafe) {
                throw new BizException("distributeSafe cant use this func");
            }

            CommonCountTotal cache = redisAbout.getRedisProvider().get(
                    DistributedKeyProvider.KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), JsonHelper.writeValueAsString(this.biz)),
                    CommonCountTotal.class
            );
            if (cache != null && this.currentDate.equals(cache.getLatestCacheDate()) && this.currentDate.equals(this.bizDate.getDate())) {
                this.commonCountTotal = cache;
                return;
            }

            String collectionName = BaseModel.getAccordCollectionNameByData(mongoTemplate, CommonCountTotal.splitKeyOf(totalDbClazz, this.biz));

            DefaultResourceInterface<CommonCountTotal> entity = new DefaultResourceInterface<>() {
                @Override
                public DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> getLock() {
                    return DistributedKeyProvider.KeyEntity.of(
                            redisAbout.getCommonCountTotalCreateLockType(),
                            JsonHelper.writeValueAsString(biz)
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
                    CommonCountTotal data = CommonCountTotal.of(totalDbClazz, bizDate);
                    data.setDistributeSafe(false);

                    return mongoTemplate.insert(data, collectionName);
                }
            };

            this.commonCountTotal = entity.getDefault(redisAbout.getRedisProvider(), o -> {
                boolean needUpdate = o.checkNeedUpdateCache(cache);

                if (needUpdate) {
                    redisAbout.getRedisProvider().set(
                            DistributedKeyProvider.KeyEntity.of(redisAbout.getCommonCountTotalCacheKeyType(), JsonHelper.writeValueAsString(this.biz)),
                            RedisProvider.AcceptType.of(JsonHelper.writeValueAsString(o)),
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
                this.beforeLatestCacheTotal = beforeLatestCacheTotal;
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
                Long value = redisAbout.getRedisProvider().get(
                        DistributedKeyProvider.KeyEntity.of(
                                redisAbout.getCountBizDateCacheKeyType(),
                                JsonHelper.writeValueAsString(this.biz.convertToBizDate(this.beforeLatestCacheDate))
                        ),
                        Long.class
                );
                if (value == null) {
                    value = 0L;
                }

                this.beforeLatestCacheTotal = value;
                this.nextCacheDate = this.commonCountTotal.getLatestCacheDate();
                return;
            }

            this.nextCacheDate = this.bizDate.getDate();
            //代表日志的日期等于上次缓存日期
            if (CacheStateEnum.STAY.equals(this.cacheState)) {
                return;
            }

            //代表日志在上次缓存日期之后
            Long value = redisAbout.getRedisProvider().get(
                    DistributedKeyProvider.KeyEntity.of(
                            redisAbout.getCountBizDateCacheKeyType(),
                            JsonHelper.writeValueAsString(this.biz.convertToBizDate(this.beforeLatestCacheDate))
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
    @JsonPropertyOrder({"typeId", "bizType", "subBizType", "date"})
    public static class CountBizDate {
        private String typeId;

        private String bizType;

        private String subBizType;

        private String date;

        public CountBiz extractBiz() {
            CountBiz data = new CountBiz();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);

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
    @JsonPropertyOrder({"typeId", "bizType", "subBizType"})
    public static class CountBiz {
        private String typeId;

        private String bizType;

        private String subBizType;

        public CountBizDate convertToBizDate(String date) {
            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(date);

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
            @CompoundIndex(name = "distributeSafe_latestCacheStamp_dataIsCold", def = "{'ds': 1, 'st':1, 'c':1}")
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

        @Field(value = "ds")
        private Boolean distributeSafe = true;

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
            CountBiz data = new CountBiz();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);

            return data;
        }

        public CountBizDate convertToBizDate(String date) {
            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(date);

            return data;
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

        public static <M extends CommonCountTotal> M of(Class<M> clazz, CountBizDate bizDate) {
            M data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
                data.setTypeId(bizDate.getTypeId());
                data.setBizType(bizDate.getBizType());
                data.setSubBizType(bizDate.getSubBizType());
                data.setTotal(0L);
                data.setLatestCacheDate(bizDate.getDate());
                data.setDataIsCold(false);
                data.setDistributeSafe(true);
            } catch (Exception e) {
                throw new BizException(String.format("createTotal fail: %s", e.getMessage()));
            }

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

        @Override
        public String calSplitIndex() {
            Date date = DateHelper.parseStringDate(this.date, DateHelper.DateFormatEnum.fullUntilDay);

            return DateHelper.dateToString(date, DateHelper.DateFormatEnum.fullUntilMonth);
        }

        public CountBiz extractBiz() {
            CountBiz data = new CountBiz();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);

            return data;
        }

        public CountBizDate extractBizDate() {
            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(this.date);

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
            CountBizDate data = new CountBizDate();

            data.setTypeId(this.typeId);
            data.setBizType(this.bizType);
            data.setSubBizType(this.subBizType);
            data.setDate(this.date);

            return data;
        }
    }


    @Setter
    @Getter
    public static class RedisAbout<M extends DistributedKeyType> {
        private RedisProvider redisProvider;

        private Boolean cacheAfterAllTotal = true;

        private Duration cacheDuration;

        private M commonCountTotalCacheKeyType;

        private M commonCountTotalCreateLockType;

        private M countBizDateCacheKeyType;

        private M commonCountPersistHistoryLockType;

        private M countBizAfterAllTotalCacheKeyType;

        private DistributedKeyProvider.KeyEntity<M> countHistoryHashKey;

        public static <M extends DistributedKeyType> RedisAbout<M> of(
                RedisProvider redisProvider, boolean cacheAfterAllTotal, Duration cacheDuration,
                M commonCountTotalCacheKeyType, M commonCountTotalCreateLockType, M countBizDateCacheKeyType,
                M commonCountPersistHistoryLockType, M countBizAfterAllTotalCacheKeyType, DistributedKeyProvider.KeyEntity<M> countHistoryHashKey
        ) {
            RedisAbout<M> data = new RedisAbout<>();

            data.setRedisProvider(redisProvider);
            data.setCacheAfterAllTotal(cacheAfterAllTotal);
            data.setCacheDuration(cacheDuration);
            data.setCommonCountTotalCacheKeyType(commonCountTotalCacheKeyType);
            data.setCommonCountTotalCreateLockType(commonCountTotalCreateLockType);
            data.setCountBizDateCacheKeyType(countBizDateCacheKeyType);
            data.setCommonCountPersistHistoryLockType(commonCountPersistHistoryLockType);
            data.setCountBizAfterAllTotalCacheKeyType(countBizAfterAllTotalCacheKeyType);
            data.setCountHistoryHashKey(countHistoryHashKey);

            return data;
        }
    }
}
