package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.mongodb.basic.IUidGenerator;
import io.github.chaogeoop.base.business.threadlocals.PrimaryChooseHolder;
import io.github.chaogeoop.base.business.mongodb.basic.BaseSpringDataMongodbQuery;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.MongoTemplate;

public interface IPrimaryChoose<M extends BaseModel> extends InitializingBean {
    @Override
    default void afterPropertiesSet() {
        PrimaryChooseHelper.fillDatabaseMainMap(this.getPrimary(), this.getPrimary());
        PrimaryChooseHelper.fillDatabaseMainMap(this.getSlaver(), this.getPrimary());
    }

    MongoTemplate getPrimary();

    MongoTemplate getSlaver();

    Class<M> getModel();

    default MongoTemplate getAccord() {
        boolean choosePrimary = PrimaryChooseHolder.get();
        if (choosePrimary) {
            return this.getPrimary();
        }

        return this.getSlaver();
    }

    default IUidGenerator getUidGenerator() {
        return null;
    }

    default MongoIdEntity getMongoIdEntity() {
        return PrimaryChooseHelper.getMongoIdEntity(this);
    }

    default BaseSpringDataMongodbQuery<M> getMongoQueryBuilder() {
        return PrimaryChooseHelper.getMongoQueryBuilder(this);
    }
}
