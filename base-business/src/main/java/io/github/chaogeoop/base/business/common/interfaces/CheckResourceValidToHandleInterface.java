package io.github.chaogeoop.base.business.common.interfaces;

import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.redis.DistributedKeyType;

import javax.lang.model.type.NullType;
import java.time.Duration;
import java.util.function.Function;

public interface CheckResourceValidToHandleInterface<T> {
    DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> getLock();

    T findResource();

    boolean validToHandle(T resource);

    default <M> M handle(RedisProvider redisProvider, Function<T, M> func, Function<M, NullType> persistFunc) {
        T resource = this.findResource();
        if (resource == null || !this.validToHandle(resource)) {
            return null;
        }
        return redisProvider.exeFuncWithLock(this.getLock(), Duration.ofSeconds(10), 10, o -> {
            T obj = this.findResource();
            if (obj == null || !this.validToHandle(obj)) {
                return null;
            }

            M result = func.apply(obj);

            persistFunc.apply(result);

            return result;
        });
    }
}
