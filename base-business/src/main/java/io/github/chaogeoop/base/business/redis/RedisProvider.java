package io.github.chaogeoop.base.business.redis;

import io.github.chaogeoop.base.business.common.errors.DistributedLockedException;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.business.common.helpers.SleepHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import javax.annotation.Nullable;
import javax.lang.model.type.NullType;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class RedisProvider {
    private final RedisTemplate<String, Object> template;
    private final DistributedKeyProvider distributedKeyProvider;

    private static final String COMPARE_THEN_DEL_LUA = "if redis.call(\"get\",KEYS[1]) == ARGV[1] " +
            "then " +
            "    return redis.call(\"del\",KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end ";

    private static final String MULTI_SET_EXPIRE = "local ttl = tonumber(ARGV[#ARGV]) \n" +
            "for i, key in ipairs(KEYS) do  \n" +
            "    redis.call(\"SETEX\", key, ttl, ARGV[i])  \n" +
            "end  \n" +
            "return 1";

    public RedisProvider(RedisTemplate<String, Object> template, DistributedKeyProvider distributedKeyProvider) {
        this.template = template;
        this.distributedKeyProvider = distributedKeyProvider;
    }

    public String convertKeyEntityToString(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity) {
        return this.distributedKeyProvider.getKey(keyEntity);
    }

    public RedisTemplate<String, Object> giveTemplate() {
        return this.template;
    }

    //common
    public boolean expire(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, long timeout, TimeUnit timeUnit) {
        Boolean success = this.template.expire(this.distributedKeyProvider.getKey(keyEntity), timeout, timeUnit);
        return Boolean.TRUE.equals(success);
    }

    public boolean delete(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity) {
        return Boolean.TRUE.equals(this.template.delete(this.distributedKeyProvider.getKey(keyEntity)));
    }

    public boolean exists(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity) {
        return Boolean.TRUE.equals(this.template.hasKey(this.distributedKeyProvider.getKey(keyEntity)));
    }

    public Long delete(Set<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>> keys) {
        return this.template.delete(CollectionHelper.map(keys, this.distributedKeyProvider::getKey));
    }

    //valueOperator
    public <T> T get(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Class<T> clazz) {
        Object value = this.template.opsForValue().get(this.distributedKeyProvider.getKey(keyEntity));
        if (value == null) {
            return null;
        }

        return JsonHelper.readValue(value.toString(), clazz);
    }

    public void set(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, AcceptType type, Duration duration) {
        this.template.opsForValue().set(this.distributedKeyProvider.getKey(keyEntity), type.getValue(), duration);
    }

    public void set(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, AcceptType type) {
        this.template.opsForValue().set(this.distributedKeyProvider.getKey(keyEntity), type.getValue());
    }

    public void setEx(Map<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>, AcceptType> map, Duration duration) {
        Map<String, Object> valueMap = new HashMap<>(map.size());
        for (Map.Entry<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>, AcceptType> entry : map.entrySet()) {
            valueMap.put(this.distributedKeyProvider.getKey(entry.getKey()), entry.getValue().getValue());
        }

        this.setExIntern(valueMap, duration);
    }

    private void setExIntern(Map<String, Object> map, Duration duration) {
        if (map.isEmpty()) {
            return;
        }

        List<String> keys = new ArrayList<>(map.size());
        Object[] values = new Object[map.size() + 1];
        values[values.length - 1] = duration.toSeconds();

        List<Map.Entry<String, Object>> list = Lists.newArrayList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            Map.Entry<String, Object> entry = list.get(i);

            keys.add(entry.getKey());
            values[i] = entry.getValue();
        }

        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(MULTI_SET_EXPIRE, Long.class);
        this.template.execute(redisScript, keys, values);
    }

    public long incrBy(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, long increment) {
        Long value = this.template.opsForValue().increment(this.distributedKeyProvider.getKey(keyEntity), increment);
        if (value == null) {
            value = 0L;
        }

        return value;
    }

    public <T> List<T> multiGet(List<DistributedKeyProvider.KeyEntity<? extends DistributedKeyType>> keys, Class<T> clazz) {
        List<Object> values = this.template.opsForValue().multiGet(CollectionHelper.map(keys, this.distributedKeyProvider::getKey));
        if (values == null) {
            return CollectionHelper.map(keys, o -> null);
        }

        List<T> list = new ArrayList<>(values.size());

        for (Object value : values) {
            if (value == null) {
                list.add(null);
                continue;
            }

            list.add(JsonHelper.readValue(value.toString(), clazz));
        }

        return list;
    }

    //setOperator
    public <T> T spop(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Class<T> clazz) {
        Object value = this.template.opsForSet().pop(this.distributedKeyProvider.getKey(keyEntity));
        if (value == null) {
            return null;
        }

        return JsonHelper.readValue(value.toString(), clazz);
    }

    public <T> List<T> spop(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, int count, Class<T> clazz) {
        List<T> results = new ArrayList<>(count);

        List<Object> values = this.template.opsForSet().pop(this.distributedKeyProvider.getKey(keyEntity), count);
        if (values == null) {
            for (int i = 0; i < count; i++) {
                results.add(null);
            }

            return results;
        }

        for (Object value : values) {
            if (value == null) {
                results.add(null);
                continue;
            }

            results.add(JsonHelper.readValue(value.toString(), clazz));
        }

        return results;
    }

    public long sadd(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, AcceptType type) {
        Long num = this.template.opsForSet().add(this.distributedKeyProvider.getKey(keyEntity), type.getValue());
        if (num == null) {
            num = 0L;
        }

        return num;
    }

    public long sadd(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, List<AcceptType> types) {
        Long num = this.template.opsForSet().add(this.distributedKeyProvider.getKey(keyEntity), CollectionHelper.map(types, AcceptType::getValue).toArray());
        if (num == null) {
            num = 0L;
        }

        return num;
    }

    public Set<Object> smembers(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity) {
        Set<Object> members = this.template.opsForSet().members(this.distributedKeyProvider.getKey(keyEntity));
        if (members == null) {
            members = new HashSet<>();
        }

        return members;
    }


    //hashOperator
    public <T> List<T> hmget(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, List<String> hashKeys, Class<T> clazz) {
        HashOperations<String, String, Object> hashOperator = this.template.opsForHash();

        List<Object> values = hashOperator.multiGet(this.distributedKeyProvider.getKey(keyEntity), hashKeys);
        List<T> results = new ArrayList<>(values.size());

        for (Object value : values) {
            if (value == null) {
                results.add(null);
                continue;
            }

            results.add(JsonHelper.readValue(value.toString(), clazz));
        }

        return results;
    }

    public void hmset(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Map<String, AcceptType> map) {
        Map<String, Object> valueMap = new HashMap<>(map.size());

        for (Map.Entry<String, AcceptType> entry : map.entrySet()) {
            valueMap.put(entry.getKey(), entry.getValue().getValue());
        }

        this.template.opsForHash().putAll(this.distributedKeyProvider.getKey(keyEntity), valueMap);
    }

    public Long hdel(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Set<String> hashKeys) {
        return this.template.opsForHash().delete(this.distributedKeyProvider.getKey(keyEntity), hashKeys.toArray());
    }

    public <T> T hget(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, String hashKey, Class<T> clazz) {
        Object result = this.template.opsForHash().get(this.distributedKeyProvider.getKey(keyEntity), hashKey);
        if (result == null) {
            return null;
        }

        return JsonHelper.readValue(result.toString(), clazz);
    }


    //application
    private boolean lock(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, String lockValue, Duration duration) {
        Boolean locked = this.template.opsForValue().setIfAbsent(this.distributedKeyProvider.getKey(keyEntity), lockValue, duration);
        if (!Boolean.TRUE.equals(locked)) {
            return false;
        }

        return true;
    }

    private boolean releaseLock(DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, String lockValue) {
        RedisScript<Long> redisScript = new DefaultRedisScript<>(COMPARE_THEN_DEL_LUA, Long.class);
        Long result = this.template.execute(redisScript, Lists.newArrayList(this.distributedKeyProvider.getKey(keyEntity)), lockValue);
        return result != null && result > 0;
    }

    public <T, M extends DistributedKeyType> T exeFuncWithLock(
            DistributedKeyProvider.KeyEntity<M> keyEntity, Function<NullType, T> func
    ) {
        return this.exeFuncWithLock(keyEntity, Duration.ofSeconds(10), 10, func);
    }

    public <T> T exeFuncWithLock(
            DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Duration timeout, int maxRetry, Function<NullType, T> func
    ) {
        int time = 0;

        String lockValue = UUID.randomUUID().toString();

        boolean acquiredLock = false;
        do {
            acquiredLock = this.lock(keyEntity, lockValue, timeout);
            if (acquiredLock) {
                break;
            }

            time++;
            SleepHelper.sleep(Duration.ofMillis(200));
        } while (time < maxRetry);

        if (!acquiredLock) {
            throw new DistributedLockedException("cant get lock");
        }

        try {
            return func.apply(null);
        } catch (Exception e) {
            throw e;
        } finally {
            this.releaseLock(keyEntity, lockValue);
        }
    }

    //以下方法只做为缓存用,值都被保存为string,不要对他们进行数字加减操作
    public <T> T getDataFromCache(
            DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Duration timeout, Class<T> clazz, Function<NullType, T> func
    ) {
        T obj = this.get(keyEntity, clazz);

        if (obj == null) {
            obj = func.apply(null);
            if (obj != null) {
                this.set(keyEntity, AcceptType.of(JsonHelper.writeValueAsString(obj)), timeout);
            }
        }

        return obj;
    }

    public <T> List<T> getListDataFromCache(
            DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity, Duration timeout, TypeReference<List<T>> type,
            Function<NullType, List<T>> func
    ) {
        List<T> list = null;

        String jsonStr = this.get(keyEntity, String.class);

        if (jsonStr == null) {
            list = func.apply(null);
            if (list != null) {
                this.set(keyEntity, AcceptType.of(JsonHelper.writeValueAsString(list)), timeout);
            }
        } else {
            list = JsonHelper.readValue(jsonStr, type);
        }

        return list;
    }

    public <K, V, T extends DistributedKeyType> Map<K, V> getMapFromValueCache(
            T type, Duration timeout, Class<V> clazz, List<K> ids, Function<List<K>, Map<K, V>> func
    ) {
        Map<K, V> map = new HashMap<>(ids.size());
        if (CollectionHelper.isEmpty(ids)) {
            return map;
        }

        List<K> needCacheIds = new ArrayList<>();

        List<V> cacheList = this.multiGet(
                CollectionHelper.map(
                        ids,
                        o -> DistributedKeyProvider.KeyEntity.of(type, JsonHelper.writeValueAsString(o))
                ),
                clazz
        );

        for (int i = 0; i < cacheList.size(); i++) {
            V value = cacheList.get(i);
            K key = ids.get(i);

            if (value == null) {
                needCacheIds.add(key);
                continue;
            }

            map.put(key, value);
        }

        Map<K, V> needCacheMap = func.apply(needCacheIds);
        if (needCacheMap.isEmpty()) {
            return map;
        }

        map.putAll(needCacheMap);

        Map<String, Object> stringCacheMap = new HashMap<>(needCacheMap.size());
        for (Map.Entry<K, V> entry : needCacheMap.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            DistributedKeyProvider.KeyEntity<T> keyEntity = DistributedKeyProvider.KeyEntity.of(type, JsonHelper.writeValueAsString(entry.getKey()));

            stringCacheMap.put(this.distributedKeyProvider.getKey(keyEntity), JsonHelper.writeValueAsString(entry.getValue()));
        }
        if (!stringCacheMap.isEmpty()) {
            this.setExIntern(stringCacheMap, timeout);
        }

        return map;
    }

    public <K, V> Map<K, V> getMapFromHashCache(
            List<K> ids, Class<V> clazz, DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity,
            Duration duration, Function<List<K>, Map<K, V>> func
    ) {
        return this.getMapFromHashCache(ids, clazz, keyEntity, (int) duration.getSeconds(), TimeUnit.SECONDS, func);
    }

    public <K, V> Map<K, V> getMapFromHashCache(
            List<K> ids, Class<V> clazz, DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> keyEntity,
            int timeout, TimeUnit timeUnit, Function<List<K>, Map<K, V>> func
    ) {
        Map<K, V> map = new HashMap<>(ids.size());
        List<K> needCacheIds = new ArrayList<>();

        List<V> cacheList = this.hmget(keyEntity, CollectionHelper.map(ids, JsonHelper::writeValueAsString), clazz);

        for (int i = 0; i < ids.size(); i++) {
            K key = ids.get(i);
            V value = cacheList.get(i);
            if (value != null) {
                map.put(key, value);
                continue;
            }

            needCacheIds.add(key);
        }

        Map<K, V> needCacheMap = func.apply(needCacheIds);
        if (needCacheMap.isEmpty()) {
            return map;
        }

        map.putAll(needCacheMap);

        Map<String, String> stringCacheMap = new HashMap<>(needCacheMap.size());
        for (Map.Entry<K, V> entry : needCacheMap.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            stringCacheMap.put(JsonHelper.writeValueAsString(entry.getKey()), JsonHelper.writeValueAsString(entry.getValue()));
        }

        String hashKey = this.distributedKeyProvider.getKey(keyEntity);
        if (!stringCacheMap.isEmpty()) {
            this.template.opsForHash().putAll(hashKey, stringCacheMap);
        }
        this.template.expire(hashKey, timeout, timeUnit);

        return map;
    }


    @Getter
    public static class AcceptType {
        private Object value;

        private AcceptType() {

        }

        public static AcceptType of(Long value) {
            AcceptType data = new AcceptType();

            data.value = value;

            return data;
        }

        public static AcceptType of(Integer value) {
            AcceptType data = new AcceptType();

            data.value = value;

            return data;
        }

        public static AcceptType of(String value) {
            AcceptType data = new AcceptType();

            data.value = value;

            return data;
        }
    }
}
