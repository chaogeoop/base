package io.github.chaogeoop.base.example.app.services;

import io.github.chaogeoop.base.business.elasticsearch.*;
import io.github.chaogeoop.base.business.mongodb.MongoPersistEntity;
import io.github.chaogeoop.base.example.common.dao.EsTestDao;
import io.github.chaogeoop.base.example.repository.domains.EsTest;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class EsService {
    @Autowired
    private EsTestDao esTestDao;

    @Autowired
    private EsProvider esProvider;

    public List<EsTest> findTest(EsTestFinder obj) {
        Query query = this.esTestDao.getMongoQueryBuilder().buildQuery(obj.getQuery());

        EsHelper.SearchInput searchInput = EsHelper.SearchInput.of(query, obj.getWord(), EsTest.class);

        return this.esTestDao.search(this.esProvider, searchInput, Lists.newArrayList(obj.getFamilyId()));
    }

    public void deleteIndexList() {
        List<Class<? extends ISearch<? extends IBaseEs>>> clazzList = Lists.newArrayList(EsTest.class);

        for (Class<? extends ISearch<? extends IBaseEs>> clazz : clazzList) {
            this.esProvider.deleteIndex(clazz);
        }
    }

    public MongoPersistEntity.PersistEntity inputAndSave(EsTest input) {
        MongoPersistEntity.PersistEntity persistEntity = new MongoPersistEntity.PersistEntity();

        long uid = this.esTestDao.getMongoIdEntity().nextUid();
        input.setUid(uid);

        persistEntity.getDatabase().insert(input);

        return persistEntity;
    }

    @Setter
    @Getter
    public static class EsTestInput {
        private Long familyId;

        private String name;

        private List<Long> tagIds = new ArrayList<>();

        private EsTest.Address address;

        private List<EsTest.Address> addressList = new ArrayList<>();
    }

    @Setter
    @Getter
    public static class EsTestFinder {
        private Long familyId;

        private String word;

        private Map<String, Object> query;
    }
}

