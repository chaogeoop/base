package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;
import io.github.chaogeoop.base.business.mongodb.basic.MongoHelper;
import com.querydsl.core.types.Predicate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public interface ISplitPrimaryChooseRepository<M extends BaseModel & ISplitCollection> extends IPrimaryChoose<M> {
    default M findFirst(Predicate predicate, M splitKey) {
        return this.findFirst(predicate, splitKey, null);
    }

    default M findFirst(Predicate predicate, M splitKey, Sort sort) {
        Query query = this.getMongoQueryBuilder().buildQuery(predicate);

        return this.findFirst(query, splitKey, sort);
    }

    default M findFirst(Query query, M splitKey) {
        return this.findFirst(query, splitKey, null);
    }

    default M findFirst(Query query, M splitKey, Sort sort) {
        Class<M> clazz = this.getModel();
        String collectionName = PrimaryChooseHelper.calCollectionName(this, splitKey);

        return MongoHelper.findFirst(this.getAccord(), query, sort, clazz, collectionName);
    }

    default List<M> listQuery(Predicate predicate, List<M> splitKeys) {
        return this.listQuery(predicate, splitKeys, new ArrayList<>());
    }

    default List<M> listQuery(Predicate predicate, List<M> splitKeys, List<String> fields) {
        Query query = this.getMongoQueryBuilder().buildQuery(predicate);

        return this.listQuery(query, splitKeys, fields);
    }

    default List<M> listQuery(Query query, List<M> splitKeys) {
        return this.listQuery(query, splitKeys, new ArrayList<>());
    }

    default List<M> listQuery(Query query, List<M> splitKeys, List<String> fields) {
        Set<String> collectionNames = PrimaryChooseHelper.calCollectionNames(this, splitKeys);

        List<M> list = new ArrayList<>();

        for (String collectionName : collectionNames) {
            list.addAll(MongoHelper.listQuery(this.getAccord(), query, fields, null, this.getModel(), collectionName));
        }

        return list;
    }
}
