package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.elasticsearch.*;
import io.searchbox.client.JestClient;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

public class InitCollectionIndexHandler {
    private final List<IPrimaryChoose<? extends EnhanceBaseModel>> daoList;
    private final Set<Class<? extends ISearch<? extends IBaseEs>>> searchClazzList;
    private final Map<MongoTemplate, JestClient> databaseEsMap = new HashMap<>();

    public InitCollectionIndexHandler(
            List<IPrimaryChoose<? extends EnhanceBaseModel>> daoList, Set<Class<? extends ISearch<? extends IBaseEs>>> searchClazzList,
            List<EsProvider> esProviders
    ) {
        this.daoList = daoList;
        this.searchClazzList = searchClazzList;
        for (EsProvider esProvider : esProviders) {
            databaseEsMap.put(esProvider.giveMongoTemplate(), esProvider.giveEs());
        }
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        try {
            this.setProperties(executorService);
        } finally {
            executorService.shutdown();
        }
    }

    private void setProperties(ExecutorService executorService) {
        Set<MongoTemplate> databaseList = new HashSet<>();

        Map<MongoTemplate, List<IPrimaryChoose<? extends EnhanceBaseModel>>> templateDaoListMap = CollectionHelper.groupBy(this.daoList, IPrimaryChoose::getPrimary);
        List<MongoInitUnit> mongoInitUnitList = new ArrayList<>();
        for (Map.Entry<MongoTemplate, List<IPrimaryChoose<? extends EnhanceBaseModel>>> entry : templateDaoListMap.entrySet()) {
            MongoTemplate mongoTemplate = entry.getKey();
            databaseList.add(mongoTemplate);

            MultiValueMap<String, String> baseNameListMap = new LinkedMultiValueMap<>();

            Set<String> allCollections = mongoTemplate.getCollectionNames();
            for (String name : allCollections) {
                String base = SplitCollectionHelper.calBaseCollectionName(name);
                baseNameListMap.add(base, name);
            }

            for (IPrimaryChoose<? extends EnhanceBaseModel> dao : entry.getValue()) {
                String baseCollectionName = EnhanceBaseModel.getBaseCollectionNameByClazz(dao.getModel());
                List<String> list = baseNameListMap.getOrDefault(baseCollectionName, new ArrayList<>());

                for (String collectionName : list) {
                    mongoInitUnitList.add(MongoInitUnit.of(mongoTemplate, dao.getModel(), collectionName));
                }
            }
        }
        this.init(executorService, mongoInitUnitList, unit -> EnhanceBaseModel.getAccordCollectionNamePreInit(unit.getMongoTemplate(), unit.getClazz(), unit.getCollectionName()));

        List<EsInitUnit> esInitUnitList = new ArrayList<>();
        for (Class<? extends ISearch<? extends IBaseEs>> clazz : this.searchClazzList) {
            if (!EnhanceBaseModel.class.isAssignableFrom(clazz)) {
                continue;
            }

            String baseCollectionName = EnhanceBaseModel.getBaseCollectionNameByClazz((Class<? extends EnhanceBaseModel>) clazz);

            for (MongoTemplate database : databaseList) {
                JestClient jestClient = this.databaseEsMap.get(database);
                if (jestClient == null) {
                    continue;
                }

                EsHelper.InitEsUnit initEsUnit = EsHelper.InitEsUnit.of(clazz);

                if (initEsUnit.getEsClazz().isAnnotationPresent(EsTableName.class) || !ISplitCollection.class.isAssignableFrom(clazz)) {
                    esInitUnitList.add(EsInitUnit.of(jestClient, initEsUnit.getBaseEsName(), initEsUnit.getBaseEsName(), initEsUnit.getMapping()));
                    continue;
                }

                EnhanceBaseModel.DatabaseUnit databaseUnit = EnhanceBaseModel.DatabaseUnit.of(database, baseCollectionName);

                Set<String> splitIndicates = databaseUnit.calSplitIndicates();

                for (String splitIndex : splitIndicates) {
                    esInitUnitList.add(
                            EsInitUnit.of(
                                    jestClient,
                                    initEsUnit.getBaseEsName(),
                                    SplitCollectionHelper.combineNameWithSplitIndex(initEsUnit.getBaseEsName(), splitIndex),
                                    initEsUnit.getMapping()
                            )
                    );
                }
            }
        }
        this.init(executorService, esInitUnitList, unit -> BaseEsHelper.getAccordEsNamePreInit(unit.getJestClient(), unit.getBaseEsName(), unit.getEsName(), unit.getMapping()));
    }

    private <M> void init(ExecutorService executorService, List<M> list, Function<M, ?> func) {
        List<Future<?>> futureList = new ArrayList<>();

        for (M element : list) {
            Future<?> future = executorService.submit(() -> func.apply(element));
            futureList.add(future);
        }

        for (Future<?> future : futureList) {
            try {
                future.get();
            } catch (Exception e) {
                throw new BizException(e);
            }
        }
    }

    @Setter
    @Getter
    public static class MongoInitUnit {
        private MongoTemplate mongoTemplate;

        private Class<? extends EnhanceBaseModel> clazz;

        private String collectionName;

        public static MongoInitUnit of(MongoTemplate mongoTemplate, Class<? extends EnhanceBaseModel> clazz, String collectionName) {
            MongoInitUnit data = new MongoInitUnit();

            data.setMongoTemplate(mongoTemplate);
            data.setClazz(clazz);
            data.setCollectionName(collectionName);

            return data;
        }
    }

    @Setter
    @Getter
    public static class EsInitUnit {
        private JestClient jestClient;

        private String baseEsName;

        private String esName;

        private String mapping;

        public static EsInitUnit of(JestClient jestClient, String baseEsName, String esName, String mapping) {
            EsInitUnit data = new EsInitUnit();

            data.setJestClient(jestClient);
            data.setBaseEsName(baseEsName);
            data.setEsName(esName);
            data.setMapping(mapping);

            return data;
        }
    }
}