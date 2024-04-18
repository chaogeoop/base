package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.mongodb.EnhanceBaseModel;

import java.util.List;

public interface IUidGenerator {
    List<Long> getUids(Class<? extends EnhanceBaseModel> clazz, String nameInIdCollection, long count);
}
