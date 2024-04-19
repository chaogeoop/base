package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import io.github.chaogeoop.base.business.mongodb.basic.BaseModel;
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
        for (Map.Entry<ModelClazzCollectionName, List<BaseModel>> entry : persistMap.getInsertMap().entrySet()) {
            Collection<BaseModel> datas = this.mongoTemplate.insert(entry.getValue(), entry.getKey().getCollectionName());
            entry.setValue(Lists.newArrayList(datas));
        }

        for (Map.Entry<ModelClazzCollectionName, List<BaseModel>> entry : persistMap.getSaveMap().entrySet()) {
            for (BaseModel data : entry.getValue()) {
                this.mongoTemplate.save(data, entry.getKey().getCollectionName());
            }
        }

        for (Map.Entry<ModelClazzCollectionName, List<BaseModel>> entry : persistMap.getDeleteMap().entrySet()) {
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
        MultiValueMap<ModelClazzCollectionName, BaseModel> insertMap = new LinkedMultiValueMap<>();
        MultiValueMap<ModelClazzCollectionName, BaseModel> saveMap = new LinkedMultiValueMap<>();
        MultiValueMap<ModelClazzCollectionName, BaseModel> deleteMap = new LinkedMultiValueMap<>();


        for (PersistEntity obj : list) {
            for (Map.Entry<Class<? extends BaseModel>, Set<BaseModel>> entry : obj.getDatabase().getInsertMap().entrySet()) {
                for (BaseModel data : entry.getValue()) {
                    if (data.getClass() != entry.getKey()) {
                        throw new BizException("入库错误!");
                    }

                    String collectionName = EnhanceBaseModelManager.getAccordCollectionNameByData(this.mongoTemplate, data);
                    insertMap.add(ModelClazzCollectionName.of(entry.getKey(), collectionName), data);
                }
            }

            for (Map.Entry<Class<? extends BaseModel>, Set<BaseModel>> entry : obj.getDatabase().getSaveMap().entrySet()) {
                for (BaseModel data : entry.getValue()) {
                    if (data.getClass() != entry.getKey()) {
                        throw new BizException("入库错误!");
                    }

                    String collectionName = EnhanceBaseModelManager.getAccordCollectionNameByData(this.mongoTemplate, data);
                    saveMap.add(ModelClazzCollectionName.of(entry.getKey(), collectionName), data);
                }
            }

            for (Map.Entry<Class<? extends BaseModel>, Set<BaseModel>> entry : obj.getDatabase().getDeleteMap().entrySet()) {
                for (BaseModel data : entry.getValue()) {
                    if (data.getClass() != entry.getKey()) {
                        throw new BizException("入库错误!");
                    }

                    String collectionName = EnhanceBaseModelManager.getAccordCollectionNameByData(this.mongoTemplate, data);
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
        private Map<Class<? extends BaseModel>, Set<BaseModel>> insertMap = new HashMap<>();

        private Map<Class<? extends BaseModel>, Set<BaseModel>> saveMap = new HashMap<>();

        private Map<Class<? extends BaseModel>, Set<BaseModel>> deleteMap = new HashMap<>();

        public void insert(BaseModel data) {
            Set<BaseModel> saveSet = this.saveMap.get(data.getClass());
            if (saveSet != null) {
                saveSet.remove(data);
            }

            this.getInsertMap().putIfAbsent(data.getClass(), new HashSet<>());
            this.getInsertMap().get(data.getClass()).add(data);
        }

        public void insert(Set<BaseModel> list) {
            if (list.isEmpty()) {
                return;
            }

            for (BaseModel data : list) {
                this.insert(data);
            }
        }

        public void save(BaseModel data) {
            Set<BaseModel> insertSet = this.insertMap.get(data.getClass());
            if (insertSet != null && insertSet.contains(data)) {
                return;
            }

            this.getSaveMap().putIfAbsent(data.getClass(), new HashSet<>());
            this.getSaveMap().get(data.getClass()).add(data);
        }

        public void save(Set<BaseModel> list) {
            if (list.isEmpty()) {
                return;
            }

            for (BaseModel data : list) {
                this.save(data);
            }
        }

        public void delete(BaseModel data) {
            this.getDeleteMap().putIfAbsent(data.getClass(), new HashSet<>());
            this.getDeleteMap().get(data.getClass()).add(data);
        }

        public void delete(Set<BaseModel> list) {
            if (list.isEmpty()) {
                return;
            }

            for (BaseModel data : list) {
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

        private MultiValueMap<ModelClazzCollectionName, BaseModel> insertMap = new LinkedMultiValueMap<>();

        private MultiValueMap<ModelClazzCollectionName, BaseModel> saveMap = new LinkedMultiValueMap<>();

        private MultiValueMap<ModelClazzCollectionName, BaseModel> deleteMap = new LinkedMultiValueMap<>();

        private List<CacheInterface> cacheList = new ArrayList<>();

        private List<MessageInterface> messages = new ArrayList<>();

        public boolean databaseIsEmpty() {
            return this.insertMap.isEmpty() && this.saveMap.isEmpty() && this.deleteMap.isEmpty();
        }
    }

    @Setter
    @Getter
    public static class ModelClazzCollectionName {
        private Class<? extends BaseModel> modelClazz;

        private String collectionName;

        public static ModelClazzCollectionName of(Class<? extends BaseModel> modelClazz, String collectionName) {
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
