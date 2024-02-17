package io.github.chaogeoop.base.business.mongodb;

import io.github.chaogeoop.base.business.threadlocals.IoMonitorHolder;
import io.github.chaogeoop.base.business.mongodb.basic.RootModel;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.*;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BaseModel extends RootModel {
    private static final ConcurrentHashMap<DatabaseUnit, Set<String>> databaseUnitCollectionNamesMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<DatabaseUnit, Boolean> databaseUnitHasReadMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<MongoTemplate, Boolean> autoIndexMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Class<? extends BaseModel>, List<Index>> clazzIndexListMap = new ConcurrentHashMap<>();

    public static Set<String> getAllCollectionNames(MongoTemplate mongoTemplate, String baseCollectionName) {
        mongoTemplate = PrimaryChooseHelper.getMainDatabase(mongoTemplate);
        DatabaseUnit databaseUnit = DatabaseUnit.of(mongoTemplate, baseCollectionName);

        return Sets.newHashSet(databaseUnitCollectionNamesMap.getOrDefault(databaseUnit, new HashSet<>()));
    }

    public static String getAccordCollectionNameByData(MongoTemplate mongoTemplate, BaseModel data) {
        Class<? extends BaseModel> clazz = data.getClass();

        String baseCollectionName = RootModel.getBaseCollectionNameByClazz(clazz);

        String collectionName;
        if (!(data instanceof ISplitCollection)) {
            collectionName = baseCollectionName;
        } else {
            String splitIndex = ((ISplitCollection) data).calSplitIndex();
            collectionName = SplitCollectionHelper.combineNameWithSplitIndex(baseCollectionName, splitIndex);
        }

        return getAccordCollectionNameByClazzAndCollectionName(mongoTemplate, clazz, collectionName);
    }

    public static String getBaseCollectionNameByClazz(MongoTemplate mongoTemplate, Class<? extends BaseModel> clazz) {
        return getAccordCollectionNameByClazzAndCollectionName(mongoTemplate, clazz, RootModel.getBaseCollectionNameByClazz(clazz));
    }

    private static String getAccordCollectionNameByClazzAndCollectionName(MongoTemplate mongoTemplate, Class<? extends BaseModel> clazz, String collectionName) {
        return getAccordCollectionNameWithCheckPreInit(mongoTemplate, clazz, collectionName, false);
    }

    protected static String getAccordCollectionNamePreInit(MongoTemplate mongoTemplate, Class<? extends BaseModel> clazz, String collectionName) {
        return getAccordCollectionNameWithCheckPreInit(mongoTemplate, clazz, collectionName, true);
    }

    private static String getAccordCollectionNameWithCheckPreInit(
            MongoTemplate mongoTemplate, Class<? extends BaseModel> clazz, String collectionName, boolean preInit
    ) {
        mongoTemplate = PrimaryChooseHelper.getMainDatabase(mongoTemplate);
        boolean correct = SplitCollectionHelper.isClazzRelativeCollection(collectionName, clazz);
        if (!correct) {
            throw new BizException("这个model和表名不相符");
        }

        if (preInit) {
            initCollection(mongoTemplate, clazz, collectionName);
        }

        String baseCollectionName = BaseModel.getBaseCollectionNameByClazz(clazz);
        DatabaseUnit databaseUnit = DatabaseUnit.of(mongoTemplate, baseCollectionName);

        if (!databaseUnitHasReadMap.containsKey(databaseUnit)) {
            mongoTemplate.findOne(new Query(), clazz);
            IoMonitorHolder.incDatabase(baseCollectionName, -1);
            databaseUnitHasReadMap.put(databaseUnit, true);
        }

        if (initialized(databaseUnit, collectionName)) {
            return collectionName;
        }

        synchronized (String.format("%s_createCollectionLock", baseCollectionName).intern()) {
            if (initialized(databaseUnit, collectionName)) {
                return collectionName;
            }

            if (!databaseUnitCollectionNamesMap.containsKey(databaseUnit)) {
                databaseUnitCollectionNamesMap.put(databaseUnit, new HashSet<>());
            }

            if (!preInit) {
                initCollection(mongoTemplate, clazz, collectionName);
            }

            databaseUnitCollectionNamesMap.get(databaseUnit).add(collectionName);
        }

        return collectionName;
    }

    private static void initCollection(MongoTemplate mongoTemplate, Class<? extends BaseModel> clazz, String collectionName) {
        int failTime = 0;
        boolean autoIndex = checkIsAutoIndex(mongoTemplate);
        while (true) {
            try {
                boolean exists = mongoTemplate.collectionExists(collectionName);
                if (!exists) {
                    mongoTemplate.createCollection(collectionName);
                }

                if (autoIndex) {
                    List<Index> indexList = getIndexListFromCache(clazz);

                    for (Index index : indexList) {
                        mongoTemplate.indexOps(collectionName).ensureIndex(index);
                    }
                }

                break;
            } catch (Exception e) {
                failTime++;
                if (failTime > 1) {
                    throw e;
                }
            }
        }
    }

    private static boolean initialized(DatabaseUnit databaseUnit, String collectionName) {
        Set<String> collectionNames = databaseUnitCollectionNamesMap.get(databaseUnit);
        if (collectionNames == null) {
            return false;
        }

        if (!collectionNames.contains(collectionName)) {
            return false;
        }

        return true;
    }

    private static Set<String> getSplitIndicates(DatabaseUnit databaseUnit) {
        Set<String> list = new HashSet<>();

        Set<String> result = databaseUnitCollectionNamesMap.get(databaseUnit);
        if (result == null) {
            return list;
        }

        for (String name : result) {
            String splitIndex = SplitCollectionHelper.calSplitIndex(name);
            if (splitIndex == null) {
                continue;
            }

            list.add(splitIndex);
        }

        return list;
    }

    private static boolean checkIsAutoIndex(MongoTemplate mongoTemplate) {
        Boolean autoIndex = autoIndexMap.get(mongoTemplate);
        if (autoIndex != null) {
            return autoIndex;
        }

        boolean value = false;
        try {
            java.lang.reflect.Field field = mongoTemplate.getConverter().getMappingContext().getClass().getDeclaredField("autoIndexCreation");
            field.setAccessible(true);

            value = (boolean) field.get(mongoTemplate.getConverter().getMappingContext());
        } catch (Exception e) {
            log.error("获取mongoTemplate是否设置自动索引失败");
        }

        autoIndexMap.put(mongoTemplate, value);

        return value;
    }

    private static List<Index> getIndexListFromCache(Class<? extends BaseModel> clazz) {
        if (!ISplitCollection.class.isAssignableFrom(clazz)) {
            return getIndexList(clazz);
        }

        List<Index> indexList = clazzIndexListMap.get(clazz);
        if (indexList == null) {
            indexList = getIndexList(clazz);

            clazzIndexListMap.put(clazz, indexList);
        }

        return indexList;
    }

    private static List<Index> getIndexList(Class<? extends BaseModel> clazz) {
        List<Index> list = new ArrayList<>();

        List<CompoundIndex> compoundIndexList = new ArrayList<>();

        Class<?> checkClazz = clazz;
        do {
            for (java.lang.reflect.Field field : checkClazz.getDeclaredFields()) {
                field.setAccessible(true);

                boolean hasIndex = field.isAnnotationPresent(Indexed.class);
                if (!hasIndex) {
                    continue;
                }

                Indexed indexAnnotation = field.getAnnotation(Indexed.class);

                String name = field.getName();
                if (field.isAnnotationPresent(Field.class)) {
                    name = field.getAnnotation(Field.class).name();
                    if (StringUtils.isBlank(name)) {
                        name = field.getAnnotation(Field.class).value();
                    }
                }

                Sort.Direction direction = Sort.Direction.ASC;
                if (IndexDirection.DESCENDING.equals(indexAnnotation.direction())) {
                    direction = Sort.Direction.DESC;
                }

                Index index = new Index();

                index.on(name, direction);
                index.named(name);
                if (indexAnnotation.background()) {
                    index.background();
                }
                if (indexAnnotation.unique()) {
                    index.unique();
                }

                list.add(index);
            }

            if (checkClazz.isAnnotationPresent(CompoundIndexes.class)) {
                compoundIndexList.addAll(Lists.newArrayList(checkClazz.getAnnotation(CompoundIndexes.class).value()));
            }

            if (checkClazz.isAnnotationPresent(CompoundIndex.class)) {
                compoundIndexList.add(checkClazz.getAnnotation(CompoundIndex.class));
            }

            checkClazz = checkClazz.getSuperclass();
        } while (!BaseModel.class.equals(checkClazz));


        for (CompoundIndex compoundIndex : compoundIndexList) {
            LinkedHashMap<String, Integer> map = JsonHelper.readValue(compoundIndex.def().replace("'", "\""), new TypeReference<>() {
            });
            if (map == null) {
                map = new LinkedHashMap<>();
            }

            Index index = new Index();

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                if (entry.getValue() > 0) {
                    index.on(entry.getKey(), Sort.Direction.ASC);
                } else {
                    index.on(entry.getKey(), Sort.Direction.DESC);
                }
            }

            index.named(compoundIndex.name());
            if (compoundIndex.background()) {
                index.background();
            }
            if (compoundIndex.unique()) {
                index.unique();
            }

            list.add(index);
        }

        return list;
    }

    @Setter
    @Getter
    protected static class DatabaseUnit {
        private MongoTemplate mongoTemplate;

        private String baseCollectionName;

        private DatabaseUnit() {

        }

        public Set<String> calSplitIndicates() {
            return getSplitIndicates(this);
        }

        public static DatabaseUnit of(MongoTemplate mongoTemplate, String baseCollectionName) {
            DatabaseUnit data = new DatabaseUnit();

            data.setMongoTemplate(mongoTemplate);
            data.setBaseCollectionName(baseCollectionName);

            return data;
        }

        public static DatabaseUnit of(MongoTemplate mongoTemplate, Class<? extends BaseModel> clazz) {
            return of(mongoTemplate, BaseModel.getBaseCollectionNameByClazz(clazz));
        }

        public static <M extends IPrimaryChoose<? extends BaseModel>> DatabaseUnit of(M choose) {
            return of(choose.getPrimary(), choose.getModel());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DatabaseUnit that = (DatabaseUnit) o;
            return Objects.equals(mongoTemplate, that.mongoTemplate) && Objects.equals(baseCollectionName, that.baseCollectionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mongoTemplate, baseCollectionName);
        }
    }
}
