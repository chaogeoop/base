package io.github.chaogeoop.base.business.common.entities;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
public class IoStatistic {
    private String funcName;

    private Map<String, Long> database = new HashMap<>();

    private Map<String, Long> count = new HashMap<>();

    private Map<String, Long> redis = new HashMap<>();

    public static IoStatistic of(String funcName) {
        IoStatistic data = new IoStatistic();

        data.setFuncName(funcName);

        return data;
    }
}
