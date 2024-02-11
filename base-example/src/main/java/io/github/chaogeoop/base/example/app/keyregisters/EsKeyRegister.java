package io.github.chaogeoop.base.example.app.keyregisters;

import io.github.chaogeoop.base.business.redis.DistributedKeyType;
import io.github.chaogeoop.base.business.redis.IKeyRegister;

import java.util.ArrayList;
import java.util.List;

public class EsKeyRegister implements IKeyRegister<EsKeyRegister.EsDistributedKey> {
    public static EsDistributedKey SYNC_DATA_TO_ES_LOCK_TYPE =
            EsDistributedKey.of("syncDataToEs", "syncLog");

    public static EsDistributedKey ES_RESOURCE_FOR_JUDGE_UPDATE_CACHE_TYPE =
            EsDistributedKey.of("esResourceCacheForJudgeUpdate", "resourceUnique");

    @Override
    public List<EsDistributedKey> register() {
        List<EsDistributedKey> list = new ArrayList<>();

        list.add(SYNC_DATA_TO_ES_LOCK_TYPE);
        list.add(ES_RESOURCE_FOR_JUDGE_UPDATE_CACHE_TYPE);

        return list;
    }

    public static class EsDistributedKey extends DistributedKeyType {
        public static EsDistributedKey of(String type, String subType) {
            EsDistributedKey data = new EsDistributedKey();

            data.setType(type);
            data.setSubType(subType);

            return data;
        }
    }
}
