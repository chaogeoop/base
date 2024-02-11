package io.github.chaogeoop.base.example.app.keyregisters;

import io.github.chaogeoop.base.business.redis.DistributedKeyType;
import io.github.chaogeoop.base.business.redis.IKeyRegister;

import java.util.ArrayList;
import java.util.List;

public class IoKeyRegister implements IKeyRegister<IoKeyRegister.IoDistributedKey> {
    public static IoDistributedKey IO_MONITOR_LOG_PERSIST_LOCK_TYPE =
            IoDistributedKey.of("ioMonitorLog", "persist");

    @Override
    public List<IoDistributedKey> register() {
        List<IoDistributedKey> list = new ArrayList<>();

        list.add(IO_MONITOR_LOG_PERSIST_LOCK_TYPE);

        return list;
    }

    public static class IoDistributedKey extends DistributedKeyType {
        public static IoDistributedKey of(String type, String subType) {
            IoDistributedKey data = new IoDistributedKey();

            data.setType(type);
            data.setSubType(subType);

            return data;
        }
    }
}
