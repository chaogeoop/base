package io.github.chaogeoop.base.example.common.dao;

import io.github.chaogeoop.base.business.common.entities.PageSplitter;
import io.github.chaogeoop.base.business.elasticsearch.EsHelper;
import io.github.chaogeoop.base.business.elasticsearch.EsProvider;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.mongodb.ISplitPrimaryChooseRepository;
import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.example.repository.domains.EsTest;
import io.github.chaogeoop.base.example.repository.domains.QEsTest;
import io.github.chaogeoop.base.example.repository.es.EsTestInEs;
import com.querydsl.core.BooleanBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
public class EsTestDao implements ISplitPrimaryChooseRepository<EsTest> {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("slaverMongoTemplate")
    private MongoTemplate slaverMongoTemplate;


    @Override
    public MongoTemplate getPrimary() {
        return mongoTemplate;
    }

    @Override
    public MongoTemplate getSlaver() {
        return slaverMongoTemplate;
    }

    @Override
    public Class<EsTest> getModel() {
        return EsTest.class;
    }

    public List<EsTest> findByIds(List<Long> ids, List<Long> familyIds) {
        MongoTemplate accordMongoTemplate = this.getAccord();

        List<EsTest> items = new ArrayList<>();

        QEsTest qe = QEsTest.esTest;

        Map<String, List<Long>> collectionNameFamilyIdsMap = CollectionHelper.groupBy(
                familyIds, o -> BaseModel.getAccordCollectionNameByData(this.getPrimary(), EsTest.splitKeyOf(o))
        );

        for (Map.Entry<String, List<Long>> entry : collectionNameFamilyIdsMap.entrySet()) {
            BooleanBuilder bb = new BooleanBuilder();

            bb.and(qe.familyId.in(entry.getValue()));
            bb.and(qe.uid.in(ids));

            Query query = this.getMongoQueryBuilder().buildQuery(bb);

            items.addAll(accordMongoTemplate.find(query, EsTest.class, entry.getKey()));
        }

        items.sort(Comparator.comparing(o -> ids.indexOf(o.getUid())));

        return items;
    }

    public List<EsTest> search(EsProvider esProvider, EsHelper.SearchInput input, List<Long> familyIds) {
        BoolQueryBuilder mainQuery = input.convertToEsQuery();

        PageSplitter pageSplitter = PageSplitter.of(0, 10, "-_score");

        List<EsTest> splitKeys = CollectionHelper.map(familyIds, EsTest::splitKeyOf);

        ListPage<EsTestInEs> esDataPage = esProvider.pageQuery(mainQuery, pageSplitter, splitKeys);
        List<Long> accordIds = CollectionHelper.map(esDataPage.getList(), EsTestInEs::getUid);

        return this.findByIds(accordIds, familyIds);
    }
}
