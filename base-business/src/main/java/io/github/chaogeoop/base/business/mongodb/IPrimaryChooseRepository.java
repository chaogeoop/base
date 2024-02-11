package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.mongodb.basic.MongoHelper;
import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.entities.PageSplitter;
import com.querydsl.core.types.Predicate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public interface IPrimaryChooseRepository<M extends BaseModel> extends IPrimaryChoose<M> {
    default M findFirst(Predicate predicate) {
        return this.findFirst(predicate, null);
    }

    default M findFirst(Predicate predicate, Sort sort) {
        Query query = this.getMongoQueryBuilder().buildQuery(predicate);

        return this.findFirst(query, sort);
    }

    default M findFirst(Query query) {
        return this.findFirst(query, null);
    }

    default M findFirst(Query query, Sort sort) {
        return MongoHelper.findFirst(this.getAccord(), query, sort, PrimaryChooseHelper.getNormalModelWithCheck(this));
    }

    default long count(Predicate predicate) {
        Query query = this.getMongoQueryBuilder().buildQuery(predicate);

        return this.count(query);
    }

    default long count(Query query) {
        return MongoHelper.count(this.getAccord(), query, PrimaryChooseHelper.getNormalModelWithCheck(this));
    }

    default ListPage<M> pageQuery(Predicate predicate, PageSplitter pageSplitter) {
        return this.pageQuery(predicate, new ArrayList<>(), pageSplitter);
    }

    default ListPage<M> pageQuery(Predicate predicate, List<String> fields, PageSplitter pageSplitter) {
        Query query = this.getMongoQueryBuilder().buildQuery(predicate);

        return this.pageQuery(query, fields, pageSplitter);
    }

    default ListPage<M> pageQuery(Query query, PageSplitter pageSplitter) {
        return this.pageQuery(query, new ArrayList<>(), pageSplitter);
    }

    default ListPage<M> pageQuery(Query query, List<String> fields, PageSplitter pageSplitter) {
        return MongoHelper.pageQuery(this.getAccord(), query, fields, pageSplitter, PrimaryChooseHelper.getNormalModelWithCheck(this));
    }

    default List<M> listQuery(Predicate predicate, Sort sort) {
        return this.listQuery(predicate, new ArrayList<>(), sort);
    }

    default List<M> listQuery(Predicate predicate, List<String> fields, Sort sort) {
        Query query = this.getMongoQueryBuilder().buildQuery(predicate);

        return this.listQuery(query, fields, sort);
    }

    default List<M> listQuery(Query query, Sort sort) {
        return this.listQuery(query, new ArrayList<>(), sort);
    }

    default List<M> listQuery(Query query, List<String> fields, Sort sort) {
        return MongoHelper.listQuery(this.getAccord(), query, fields, sort, PrimaryChooseHelper.getNormalModelWithCheck(this));
    }
}
