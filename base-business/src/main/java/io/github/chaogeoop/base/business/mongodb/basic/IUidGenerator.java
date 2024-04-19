package io.github.chaogeoop.base.business.mongodb.basic;

import java.util.List;

public interface IUidGenerator {
    List<Long> getUids(Class<? extends BaseModel> clazz, String nameInIdCollection, long count);
}
