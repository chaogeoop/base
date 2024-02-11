package io.github.chaogeoop.base.business.mongodb.basic;

import io.github.chaogeoop.base.business.common.entities.PageSplitter;
import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;


public class MongoHelper {
    public static <T extends RootModel> T findFirst(
            MongoOperations mongoTemplate, Query query, Sort sort, Class<T> clazz
    ) {
        return findFirst(mongoTemplate, query, sort, clazz, RootModel.getBaseCollectionNameByClazz(clazz));
    }

    public static <T extends RootModel> T findFirst(
            MongoOperations mongoTemplate, Query query, Sort sort, Class<T> clazz, String collectionName
    ) {
        if (sort != null) {
            query.with(sort);
        }

        return mongoTemplate.findOne(query, clazz, collectionName);
    }

    public static <T extends RootModel> long count(MongoOperations mongoTemplate, Query query, Class<T> clazz) {
        return count(mongoTemplate, query, RootModel.getBaseCollectionNameByClazz(clazz));
    }

    public static long count(MongoOperations mongoTemplate, Query query, String collectionName) {
        long count;
        if (query.getQueryObject().isEmpty()) {
            count = mongoTemplate.estimatedCount(collectionName);
        } else {
            count = mongoTemplate.count(query, collectionName);
        }

        return count;
    }

    public static <T extends RootModel> ListPage<T> pageQuery(
            MongoOperations mongoTemplate, Query query, List<String> fields, PageSplitter pageSplitter, Class<T> clazz
    ) {
        return pageQuery(mongoTemplate, query, fields, pageSplitter, clazz, RootModel.getBaseCollectionNameByClazz(clazz));
    }

    public static <T extends RootModel> ListPage<T> pageQuery(
            MongoOperations mongoTemplate, Query query, List<String> fields, PageSplitter pageSplitter, Class<T> clazz, String collectionName
    ) {
        long count = count(mongoTemplate, query, collectionName);

        if (!CollectionHelper.isEmpty(fields)) {
            for (String field : fields) {
                query.fields().include(field);
            }
        }

        Sort sort = pageSplitter.calSort();
        if (sort != null) {
            query.with(sort);
        }

        query.skip(pageSplitter.getOffset());
        query.limit(pageSplitter.getLimit());

        List<T> list = mongoTemplate.find(query, clazz, collectionName);

        return ListPage.of(pageSplitter.getOffset(), pageSplitter.getLimit(), count, list);
    }

    public static <T extends RootModel> List<T> listQuery(
            MongoOperations mongoTemplate, Query query, List<String> fields, Sort sort, Class<T> clazz
    ) {
        return listQuery(mongoTemplate, query, fields, sort, clazz, RootModel.getBaseCollectionNameByClazz(clazz));
    }

    public static <T extends RootModel> List<T> listQuery(
            MongoOperations mongoTemplate, Query query, List<String> fields, Sort sort, Class<T> clazz, String collectionName
    ) {
        if (!CollectionHelper.isEmpty(fields)) {
            for (String field : fields) {
                query.fields().include(field);
            }
        }

        if (sort != null) {
            query.with(sort);
        }

        return mongoTemplate.find(query, clazz, collectionName);
    }
}
