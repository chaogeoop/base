package io.github.chaogeoop.base.business.threadlocals;

import io.github.chaogeoop.base.business.common.entities.IoStatistic;
import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoMonitorHolder {
    private static final ThreadLocal<Map<String, IoStatistic>> ioMonitorHolder = new ThreadLocal<>();

    public static void init(String funcName) {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map == null) {
            map = new HashMap<>();
            ioMonitorHolder.set(map);
        }

        if (map.containsKey(funcName)) {
            return;
        }

        map.put(funcName, IoStatistic.of(funcName));
    }

    @Nullable
    public static IoStatistic get(String funcName) {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map == null) {
            return null;
        }

        IoStatistic result = map.get(funcName);
        if (result == null) {
            return null;
        }

        List<String> zeroKeys = new ArrayList<>();

        for (Map.Entry<String, Long> entry : result.getDatabase().entrySet()) {
            if (entry.getValue() == 0) {
                zeroKeys.add(entry.getKey());
            }
        }

        for (String key : zeroKeys) {
            result.getDatabase().remove(key);
        }

        return result;
    }

    public static void removeFunc(String funcName) {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map == null) {
            return;
        }

        map.remove(funcName);
    }

    public static void incDatabase(String collectionName) {
        incDatabase(collectionName, 1);
    }

    public static void incDatabase(String collectionName, long times) {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map == null) {
            return;
        }

        for (Map.Entry<String, IoStatistic> entry : map.entrySet()) {
            IoStatistic result = entry.getValue();

            Long before = result.getDatabase().get(collectionName);
            if (before == null) {
                before = 0L;
            }

            result.getDatabase().put(collectionName, before + times);
        }
    }

    public static void incCount(String collectionName) {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map == null) {
            return;
        }

        for (Map.Entry<String, IoStatistic> entry : map.entrySet()) {
            IoStatistic result = entry.getValue();

            Long before = result.getCount().get(collectionName);
            if (before == null) {
                before = 0L;
            }

            result.getCount().put(collectionName, before + 1);
        }
    }

    public static void incRedis(String funcName) {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map == null) {
            return;
        }

        for (Map.Entry<String, IoStatistic> entry : map.entrySet()) {
            IoStatistic result = entry.getValue();

            Long before = result.getRedis().get(funcName);
            if (before == null) {
                before = 0L;
            }

            result.getRedis().put(funcName, before + 1);
        }
    }

    public static void remove() {
        Map<String, IoStatistic> map = ioMonitorHolder.get();
        if (map != null && !map.isEmpty()) {
            return;
        }

        ioMonitorHolder.remove();
    }
}
