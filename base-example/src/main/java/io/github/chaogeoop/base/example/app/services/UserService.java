package io.github.chaogeoop.base.example.app.services;

import io.github.chaogeoop.base.business.common.entities.BaseUserContext;
import io.github.chaogeoop.base.business.common.interfaces.IUserContextConverter;
import io.github.chaogeoop.base.business.mongodb.IPrimaryChooseStamp;
import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.keyregisters.UserKeyRegister;
import io.github.chaogeoop.base.example.repository.entities.UserContext;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.NativeWebRequest;

import java.time.Duration;
import java.util.Date;

@Service
public class UserService implements IUserContextConverter, IPrimaryChooseStamp {
    @Autowired
    private RedisProvider redisProvider;

    @Override
    public BaseUserContext convert(NativeWebRequest request) {
        String stringUserId = request.getHeader("userId");

        UserContext userContext = new UserContext();
        if (!StringUtils.isBlank(stringUserId)) {
            userContext.setUserId(JsonHelper.readValue(stringUserId, Long.class));
        }

        return userContext;
    }

    @Override
    public void record(BaseUserContext baseUserContext, long stamp) {
        UserContext userContext = (UserContext) baseUserContext;
        if (userContext.getUserId() == null) {
            return;
        }

        long inc = stamp - new Date().getTime();
        if (inc <= 0) {
            inc = Duration.ofSeconds(10).toMillis();
        }

        this.redisProvider.set(this.getPrimaryChooseStampKey(userContext), RedisProvider.AcceptType.of(stamp), Duration.ofMillis(inc));
    }

    @Override
    public Long read(BaseUserContext baseUserContext) {
        UserContext userContext = (UserContext) baseUserContext;
        if (userContext.getUserId() == null) {
            return null;
        }

        return this.redisProvider.get(this.getPrimaryChooseStampKey(userContext), Long.class);
    }

    private DistributedKeyProvider.KeyEntity<UserKeyRegister.UserDistributedKey> getPrimaryChooseStampKey(UserContext userContext) {
        return DistributedKeyProvider.KeyEntity.of(
                UserKeyRegister.USER_PRIMARY_CHOOSE_CACHE_TYPE,
                userContext.getUserId().toString()
        );
    }
}
