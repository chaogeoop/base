package io.github.chaogeoop.base.business.threadlocals;

import io.github.chaogeoop.base.business.common.entities.IoStatistic;
import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IoMonitorHolder {
    private static final ThreadLocal<IoStatistic> ioMonitorHolder = new ThreadLocal<>();

    public static void init(String funcName) {
        ioMonitorHolder.set(IoStatistic.of(funcName));
    }

    @Nullable
    public static IoStatistic get() {
        IoStatistic result = ioMonitorHolder.get();
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

    public static <M extends BaseModel> void incDatabase(String collectionName) {
        incDatabase(collectionName, 1);
    }

    public static <M extends BaseModel> void incDatabase(String collectionName, long times) {
        IoStatistic result = ioMonitorHolder.get();
        if (result == null) {
            return;
        }

        Long before = result.getDatabase().get(collectionName);
        if (before == null) {
            before = 0L;
        }

        result.getDatabase().put(collectionName, before + times);
    }

    public static void incCount(String collectionName) {
        IoStatistic result = ioMonitorHolder.get();
        if (result == null) {
            return;
        }

        Long before = result.getCount().get(collectionName);
        if (before == null) {
            before = 0L;
        }

        result.getCount().put(collectionName, before + 1);
    }

    public static void incRedis(String funcName) {
        IoStatistic result = ioMonitorHolder.get();
        if (result == null) {
            return;
        }

        Long before = result.getRedis().get(funcName);
        if (before == null) {
            before = 0L;
        }

        result.getRedis().put(funcName, before + 1);
    }

    public static void remove() {
        ioMonitorHolder.remove();
    }
}
