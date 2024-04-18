package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.*;

public class MongoPersistEntity {
    private final MongoTemplate mongoTemplate;

    private final List<AfterDbPersistInterface> afterDbPersistHandlers;

    private MongoPersistEntity(MongoTemplate mongoTemplate, List<AfterDbPersistInterface> afterDbPersistHandlers) {
        this.mongoTemplate = mongoTemplate;
        this.afterDbPersistHandlers = afterDbPersistHandlers;
    }

    protected static MongoPersistEntity of(MongoTemplate mongoTemplate, List<AfterDbPersistInterface> afterDbPersistHandlers) {
        return new MongoPersistEntity(mongoTemplate, afterDbPersistHandlers);
    }

    protected void persist(MongoPersistEntity.PersistMap persistMap) {
        for (Map.Entry<ModelClazzCollectionName, List<EnhanceBaseModel>> entry : persistMap.getInsertMap().entrySet()) {
            Collection<EnhanceBaseModel> datas = this.mongoTemplate.insert(entry.getValue(), entry.getKey().getCollectionName());
            entry.setValue(Lists.newArrayList(datas));
        }

        for (Map.Entry<ModelClazzCollectionName, List<EnhanceBaseModel>> entry : persistMap.getSaveMap().entrySet()) {
            for (EnhanceBaseModel data : entry.getValue()) {
                this.mongoTemplate.save(data, entry.getKey().getCollectionName());
            }
        }

        for (Map.Entry<ModelClazzCollectionName, List<EnhanceBaseModel>> entry : persistMap.getDeleteMap().entrySet()) {
            List<ObjectId> ids = CollectionHelper.map(entry.getValue(), o -> new ObjectId(o.getId().toString(16)));

            Query query = new Query();
            query.addCriteria(Criteria.where("_id").in(ids));

            this.mongoTemplate.remove(query, entry.getKey().getCollectionName());
        }

        for (AfterDbPersistInterface handler : this.afterDbPersistHandlers) {
            handler.handle(persistMap);
        }

        CachePersistEntity entity = new CachePersistEntity(persistMap.getCacheList());

        try {
            entity.persist();
        } catch (Exception e) {
            entity.rollback();

            throw e;
        }
    }

    protected PersistMap convertToCollectionNameDatabaseMap(List<PersistEntity> list) {
        MultiValueMap<ModelClazzCollectionName, EnhanceBaseModel> insertMap = new LinkedMultiValueMap<>();
        MultiValueMap<ModelClazzCollectionName, EnhanceBaseModel> saveMap = new LinkedMultiValueMap<>();
        MultiValueMap<ModelClazzCollectionName, EnhanceBaseModel> deleteMap = new LinkedMultiValueMap<>();


        for (PersistEntity obj : list) {
            for (Map.Entry<Class<? extends EnhanceBaseModel>, Set<EnhanceBaseModel>> entry : obj.getDatabase().getInsertMap().entrySet()) {
                for (EnhanceBaseModel data : entry.getValue()) {
                    if (data.getClass() != entry.getKey()) {
                        throw new BizException("入库错误!");
                    }

                    String collectionName = EnhanceBaseModel.getAccordCollectionNameByData(this.mongoTemplate, data);
                    insertMap.add(ModelClazzCollectionName.of(entry.getKey(), collectionName), data);
                }
            }

            for (Map.Entry<Class<? extends EnhanceBaseModel>, Set<EnhanceBaseModel>> entry : obj.getDatabase().getSaveMap().entrySet()) {
                for (EnhanceBaseModel data : entry.getValue()) {
                    if (data.getClass() != entry.getKey()) {
                        throw new BizException("入库错误!");
                    }

                    String collectionName = EnhanceBaseModel.getAccordCollectionNameByData(this.mongoTemplate, data);
                    saveMap.add(ModelClazzCollectionName.of(entry.getKey(), collectionName), data);
                }
            }

            for (Map.Entry<Class<? extends EnhanceBaseModel>, Set<EnhanceBaseModel>> entry : obj.getDatabase().getDeleteMap().entrySet()) {
                for (EnhanceBaseModel data : entry.getValue()) {
                    if (data.getClass() != entry.getKey()) {
                        throw new BizException("入库错误!");
                    }

                    String collectionName = EnhanceBaseModel.getAccordCollectionNameByData(this.mongoTemplate, data);
                    deleteMap.add(ModelClazzCollectionName.of(entry.getKey(), collectionName), data);
                }
            }
        }

        PersistMap persistMap = new PersistMap();
        persistMap.setMongoTemplate(this.mongoTemplate);
        persistMap.setInsertMap(insertMap);
        persistMap.setSaveMap(saveMap);
        persistMap.setDeleteMap(deleteMap);

        for (PersistEntity obj : list) {
            persistMap.getCacheList().addAll(obj.getCacheList());
            persistMap.getMessages().addAll(obj.getMessages());
        }

        return persistMap;
    }

    private static class CachePersistEntity {
        private final List<CacheInterface> list;
        private int persistIndex = 0;

        CachePersistEntity(List<CacheInterface> list) {
            this.list = list;
        }

        void persist() {
            for (CacheInterface obj : this.list) {
                obj.persist();
                this.persistIndex++;
            }
        }

        void rollback() {
            for (int i = 0; i < this.persistIndex; i++) {
                this.list.get(i).rollback();
            }
        }
    }

    @Setter
    @Getter
    public static class PersistEntity {
        private PersistDatabase database = new PersistDatabase();

        private List<CacheInterface> cacheList = new ArrayList<>();

        private List<MessageInterface> messages = new ArrayList<>();
    }

    @Setter
    @Getter
    public static class PersistDatabase {
        private Map<Class<? extends EnhanceBaseModel>, Set<EnhanceBaseModel>> insertMap = new HashMap<>();

        private Map<Class<? extends EnhanceBaseModel>, Set<EnhanceBaseModel>> saveMap = new HashMap<>();

        private Map<Class<? extends EnhanceBaseModel>, Set<EnhanceBaseModel>> deleteMap = new HashMap<>();

        public void insert(EnhanceBaseModel data) {
            Set<EnhanceBaseModel> saveSet = this.saveMap.get(data.getClass());
            if (saveSet != null) {
                saveSet.remove(data);
            }

            this.getInsertMap().putIfAbsent(data.getClass(), new HashSet<>());
            this.getInsertMap().get(data.getClass()).add(data);
        }

        public void insert(Set<EnhanceBaseModel> list) {
            if (list.isEmpty()) {
                return;
            }

            for (EnhanceBaseModel data : list) {
                this.insert(data);
            }
        }

        public void save(EnhanceBaseModel data) {
            Set<EnhanceBaseModel> insertSet = this.insertMap.get(data.getClass());
            if (insertSet != null && insertSet.contains(data)) {
                return;
            }

            this.getSaveMap().putIfAbsent(data.getClass(), new HashSet<>());
            this.getSaveMap().get(data.getClass()).add(data);
        }

        public void save(Set<EnhanceBaseModel> list) {
            if (list.isEmpty()) {
                return;
            }

            for (EnhanceBaseModel data : list) {
                this.save(data);
            }
        }

        public void delete(EnhanceBaseModel data) {
            this.getDeleteMap().putIfAbsent(data.getClass(), new HashSet<>());
            this.getDeleteMap().get(data.getClass()).add(data);
        }

        public void delete(Set<EnhanceBaseModel> list) {
            if (list.isEmpty()) {
                return;
            }

            for (EnhanceBaseModel data : list) {
                this.delete(data);
            }
        }

        public boolean isEmpty() {
            return this.insertMap.isEmpty() && this.saveMap.isEmpty() && this.deleteMap.isEmpty();
        }
    }

    @Setter
    @Getter
    public static class PersistMap {
        private MongoTemplate mongoTemplate;

        private MultiValueMap<ModelClazzCollectionName, EnhanceBaseModel> insertMap = new LinkedMultiValueMap<>();

        private MultiValueMap<ModelClazzCollectionName, EnhanceBaseModel> saveMap = new LinkedMultiValueMap<>();

        private MultiValueMap<ModelClazzCollectionName, EnhanceBaseModel> deleteMap = new LinkedMultiValueMap<>();

        private List<CacheInterface> cacheList = new ArrayList<>();

        private List<MessageInterface> messages = new ArrayList<>();

        public boolean databaseIsEmpty() {
            return this.insertMap.isEmpty() && this.saveMap.isEmpty() && this.deleteMap.isEmpty();
        }
    }

    @Setter
    @Getter
    public static class ModelClazzCollectionName {
        private Class<? extends EnhanceBaseModel> modelClazz;

        private String collectionName;

        public static ModelClazzCollectionName of(Class<? extends EnhanceBaseModel> modelClazz, String collectionName) {
            ModelClazzCollectionName data = new ModelClazzCollectionName();

            data.setModelClazz(modelClazz);
            data.setCollectionName(collectionName);

            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ModelClazzCollectionName that = (ModelClazzCollectionName) o;
            return com.google.common.base.Objects.equal(modelClazz, that.modelClazz) && com.google.common.base.Objects.equal(collectionName, that.collectionName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(modelClazz, collectionName);
        }
    }

    public interface CacheInterface {
        void persist();

        void rollback();
    }

    public interface MessageInterface {
        void send();
    }

    public interface AfterDbPersistInterface {
        void handle(PersistMap persistMap);
    }
}
