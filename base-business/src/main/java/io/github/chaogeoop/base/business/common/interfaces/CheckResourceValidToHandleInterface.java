package io.github.chaogeoop.base.business.common.interfaces;

import io.github.chaogeoop.base.business.redis.KeyEntity;
import io.github.chaogeoop.base.business.redis.StrictRedisProvider;
import io.github.chaogeoop.base.business.redis.KeyType;

import javax.lang.model.type.NullType;
import java.time.Duration;
import java.util.function.Function;

public interface CheckResourceValidToHandleInterface<T> {
    KeyEntity<? extends KeyType> getLock();

    T findResource();

    boolean validToHandle(T resource);

    default <M> M handle(StrictRedisProvider strictRedisProvider, Function<T, M> func, Function<M, NullType> persistFunc) {
        T resource = this.findResource();
        if (resource == null || !this.validToHandle(resource)) {
            return null;
        }
        return strictRedisProvider.exeFuncWithLock(this.getLock(), Duration.ofSeconds(10), 10, o -> {
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
