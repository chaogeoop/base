package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;
import io.github.chaogeoop.base.business.mongodb.basic.BaseSpringDataMongodbQuery;
import io.github.chaogeoop.base.business.common.errors.BizException;
import com.google.common.collect.Sets;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PrimaryChooseHelper {
    private static final ConcurrentHashMap<MongoTemplate, MongoTemplate> databaseMainMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<EnhanceBaseModelManager.DatabaseUnit, MongoIdEntity> modelIdEntityMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<EnhanceBaseModelManager.DatabaseUnit, BaseSpringDataMongodbQuery<? extends BaseModel>> modelQueryBuilderMap = new ConcurrentHashMap<>();

    protected static void fillDatabaseMainMap(MongoTemplate mongoTemplate, MongoTemplate mainMongoTemplate) {
        if (databaseMainMap.containsKey(mongoTemplate)) {
            return;
        }

        databaseMainMap.put(mongoTemplate, mainMongoTemplate);
    }

    protected static MongoTemplate getMainDatabase(MongoTemplate mongoTemplate) {
        MongoTemplate mainMongoTemplate = databaseMainMap.get(mongoTemplate);
        if (mainMongoTemplate != null) {
            return mainMongoTemplate;
        }

        return mongoTemplate;
    }

    protected static MongoIdEntity getMongoIdEntity(IPrimaryChoose<? extends BaseModel> choose) {
        EnhanceBaseModelManager.DatabaseUnit databaseUnit = EnhanceBaseModelManager.DatabaseUnit.of(choose);

        MongoIdEntity mongoIdEntity = modelIdEntityMap.get(databaseUnit);
        if (mongoIdEntity != null) {
            return mongoIdEntity;
        }

        synchronized (choose.getModel()) {
            mongoIdEntity = modelIdEntityMap.get(databaseUnit);
            if (mongoIdEntity != null) {
                return mongoIdEntity;
            }

            mongoIdEntity = MongoIdEntity.of(choose.getPrimary(), choose.getModel(), choose.getUidGenerator());
            modelIdEntityMap.put(databaseUnit, mongoIdEntity);
        }

        return mongoIdEntity;
    }

    protected static <M extends BaseModel, K extends IPrimaryChoose<M>> BaseSpringDataMongodbQuery<M> getMongoQueryBuilder(K choose) {
        EnhanceBaseModelManager.DatabaseUnit databaseUnit = EnhanceBaseModelManager.DatabaseUnit.of(choose);

        BaseSpringDataMongodbQuery<? extends BaseModel> mongodbQueryBuilder = modelQueryBuilderMap.get(databaseUnit);
        if (mongodbQueryBuilder != null) {
            return (BaseSpringDataMongodbQuery<M>) mongodbQueryBuilder;
        }

        synchronized (choose.getModel()) {
            mongodbQueryBuilder = modelQueryBuilderMap.get(databaseUnit);
            if (mongodbQueryBuilder != null) {
                return (BaseSpringDataMongodbQuery<M>) mongodbQueryBuilder;
            }

            mongodbQueryBuilder = new BaseSpringDataMongodbQuery<>(choose.getPrimary(), choose.getModel());
            modelQueryBuilderMap.put(databaseUnit, mongodbQueryBuilder);
        }

        return (BaseSpringDataMongodbQuery<M>) mongodbQueryBuilder;
    }

    public static <M extends BaseModel, K extends IPrimaryChoose<M>> Class<M> getNormalModelWithCheck(K dao) {
        Class<M> model = dao.getModel();
        if (ISplitCollection.class.isAssignableFrom(model)) {
            throw new BizException("分表model的dao不能实现这个接口");
        }

        return model;
    }

    public static <M extends BaseModel, K extends IPrimaryChoose<M>> String calCollectionName(K dao, M splitKey) {
        Set<String> collectionNames = calCollectionNames(dao, Set.of(splitKey));

        for (String collectionName : collectionNames) {
            return collectionName;
        }

        throw new BizException("获取表名出错");
    }


    public static <M extends BaseModel, K extends IPrimaryChoose<M>> Set<String> calCollectionNames(K dao, List<M> splitKeys) {
        return calCollectionNames(dao, Sets.newHashSet(splitKeys));
    }

    public static <M extends BaseModel, K extends IPrimaryChoose<M>> Set<String> calCollectionNames(K dao, Set<M> splitKeys) {
        Class<M> model = dao.getModel();

        Set<String> collectionNames = new HashSet<>();
        for (M splitKey : splitKeys) {
            if (model != splitKey.getClass()) {
                throw new BizException(String.format("传入的分表key错误: %s %s", model.getName(), splitKey.getClass().getName()));
            }

            collectionNames.add(EnhanceBaseModelManager.getAccordCollectionNameByData(dao.getPrimary(), splitKey));
        }

        return collectionNames;
    }
}
