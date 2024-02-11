package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.mongodb.BaseModel;

import java.util.List;

public interface IUidGenerator {
    List<Long> getUids(Class<? extends BaseModel> clazz, String nameInIdCollection, long count);
}
