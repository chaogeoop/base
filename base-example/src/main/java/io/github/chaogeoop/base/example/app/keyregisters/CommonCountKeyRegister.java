package io.github.chaogeoop.base.example.app.keyregisters;

import io.github.chaogeoop.base.business.redis.KeyEntity;
import io.github.chaogeoop.base.business.redis.KeyType;
import io.github.chaogeoop.base.business.redis.IKeyRegister;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public class CommonCountKeyRegister implements IKeyRegister<CommonCountKeyRegister.CommonCountDistributedKey> {
    public static CommonCountDistributedKey COMMON_COUNT_TOTAL_CACHE_TYPE =
            CommonCountDistributedKey.of("commonCountTotal", "cache");

    public static CommonCountDistributedKey COMMON_COUNT_TOTAL_CREATE_LOCK_TYPE =
            CommonCountDistributedKey.of("commonCountTotal", "createLock");

    public static CommonCountDistributedKey COUNT_BIZ_DATE_CACHE_TYPE =
            CommonCountDistributedKey.of("countBizDate", "cache");

    public static CommonCountDistributedKey COMMON_COUNT_PERSIST_HISTORY_PERSIST_LOCK_TYPE =
            CommonCountDistributedKey.of("commonCountPersistHistory", "persistLock");

    private static final CommonCountDistributedKey COMMON_COUNT_PERSIST_HISTORY_HASH_STORE_TYPE =
            CommonCountDistributedKey.of("commonCountPersistHistory", "hashStore");

    public static CommonCountDistributedKey COUNT_BIZ_AFTER_ALL_TOTAL_CACHE_TYPE =
            CommonCountDistributedKey.of("countBizAfterAllTotal", "cache");

    public static CommonCountDistributedKey USER_COLLECT_BOOKS_LOCK_TYPE =
            CommonCountDistributedKey.of("userCollectBooks", "lock");

    @Override
    public List<CommonCountDistributedKey> register() {
        List<CommonCountDistributedKey> list = new ArrayList<>();

        list.add(COMMON_COUNT_TOTAL_CACHE_TYPE);
        list.add(COMMON_COUNT_TOTAL_CREATE_LOCK_TYPE);
        list.add(COUNT_BIZ_DATE_CACHE_TYPE);
        list.add(COMMON_COUNT_PERSIST_HISTORY_PERSIST_LOCK_TYPE);
        list.add(COMMON_COUNT_PERSIST_HISTORY_HASH_STORE_TYPE);
        list.add(COUNT_BIZ_AFTER_ALL_TOTAL_CACHE_TYPE);
        list.add(USER_COLLECT_BOOKS_LOCK_TYPE);

        return list;
    }

    public static KeyEntity<CommonCountDistributedKey> getHistoryStoreHashKeyEntity() {
        return KeyEntity.of(CommonCountKeyRegister.COMMON_COUNT_PERSIST_HISTORY_HASH_STORE_TYPE, "hash");
    }

    @Setter
    @Getter
    public static class CommonCountDistributedKey extends KeyType {
        public static CommonCountDistributedKey of(String type, String subType) {
            CommonCountDistributedKey data = new CommonCountDistributedKey();

            data.setType(type);
            data.setSubType(subType);

            return data;
        }
    }
}
