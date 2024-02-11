package io.github.chaogeoop.base.business.common.interfaces;

import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.redis.DistributedKeyType;

import javax.lang.model.type.NullType;
import java.time.Duration;
import java.util.function.Function;

public interface DefaultResourceInterface<T> {
    DistributedKeyProvider.KeyEntity<? extends DistributedKeyType> getLock();

    T findExist();

    T createWhenNotExist();

    default T getDefault(RedisProvider redisProvider, Function<T, NullType> persistFunc) {
        T obj = this.findExist();
        if (obj != null) {
            persistFunc.apply(obj);
            return obj;
        }

        return redisProvider.exeFuncWithLock(this.getLock(), Duration.ofSeconds(10), 10, o -> {
            T exist = this.findExist();
            if (exist == null) {
                exist = this.createWhenNotExist();
            }

            persistFunc.apply(exist);

            return exist;
        });
    }
}
