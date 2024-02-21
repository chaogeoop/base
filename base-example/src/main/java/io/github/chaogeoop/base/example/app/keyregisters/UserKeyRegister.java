package io.github.chaogeoop.base.example.app.keyregisters;

import io.github.chaogeoop.base.business.redis.KeyType;
import io.github.chaogeoop.base.business.redis.IKeyRegister;

import java.util.ArrayList;
import java.util.List;

public class UserKeyRegister implements IKeyRegister<UserKeyRegister.UserDistributedKey> {
    public static UserDistributedKey USER_PRIMARY_CHOOSE_CACHE_TYPE =
            UserDistributedKey.of("user", "primaryChoose");

    @Override
    public List<UserDistributedKey> register() {
        List<UserDistributedKey> list = new ArrayList<>();

        list.add(USER_PRIMARY_CHOOSE_CACHE_TYPE);

        return list;
    }

    public static class UserDistributedKey extends KeyType {
        public static UserDistributedKey of(String type, String subType) {
            UserDistributedKey data = new UserDistributedKey();

            data.setType(type);
            data.setSubType(subType);

            return data;
        }
    }
}
