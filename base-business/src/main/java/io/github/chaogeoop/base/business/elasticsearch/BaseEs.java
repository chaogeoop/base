package io.github.chaogeoop.base.business.elasticsearch;

import io.github.chaogeoop.base.business.common.errors.BizException;
import com.google.common.collect.Sets;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BaseEs {
    private static final ConcurrentHashMap<EsUnit, Set<String>> esUnitEsNamesMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<EsUnit, String> esUnitMappingMap = new ConcurrentHashMap<>();

    public static String getAccordEsNameByData(JestClient jestClient, String baseEsName, String esName, String mapping) {
        TableEntity entity = new TableEntity(jestClient, baseEsName, esName, mapping);

        return entity.getInitializedRealEsName(false);
    }

    public static String getAccordEsNamePreInit(JestClient jestClient, String baseEsName, String esName, String mapping) {
        TableEntity entity = new TableEntity(jestClient, baseEsName, esName, mapping);

        return entity.getInitializedRealEsName(true);
    }

    public static Set<String> getAllEsNames(JestClient jestClient, String baseEsName) {
        EsUnit esUnit = EsUnit.of(jestClient, baseEsName.toLowerCase());

        return Sets.newHashSet(esUnitEsNamesMap.getOrDefault(esUnit, new HashSet<>()));
    }

    protected static void deleteIndex(JestClient jestClient, Class<? extends ISearch<? extends BaseEs>> clazz) {
        EsHelper.InitEsUnit initEsUnit = EsHelper.InitEsUnit.of(clazz);
        EsUnit esUnit = EsUnit.of(jestClient, initEsUnit.getBaseEsName());
        Set<String> esNames = Sets.newHashSet(esUnitEsNamesMap.getOrDefault(esUnit, new HashSet<>()));

        for (String esName : esNames) {
            try {
                TableEntity entity = new TableEntity(jestClient, esUnit.getBaseEsName(), esName, initEsUnit.getMapping());

                entity.deleteIndex();
            } catch (Exception e) {

            }
        }
    }

    @Nullable
    protected static String getMappingFromCache(JestClient jestClient, String baseEsName) {
        EsUnit esUnit = EsUnit.of(jestClient, baseEsName.toLowerCase());

        return esUnitMappingMap.get(esUnit);
    }

    private static class TableEntity {
        private final EsUnit esUnit;
        private final String esName;
        private final String mapping;

        TableEntity(JestClient jestClient, String baseEsName, String esName, String mapping) {
            this.esUnit = EsUnit.of(jestClient, baseEsName.toLowerCase());
            this.esName = esName.toLowerCase();
            this.mapping = mapping;
        }

        public String getInitializedRealEsName(boolean preInit) {
            if (preInit) {
                this.initIndex(this.mapping);
            }

            if (this.isInitialized()) {
                return this.esName;
            }

            synchronized (this.getLock().intern()) {
                if (this.isInitialized()) {
                    return this.esName;
                }

                if (!esUnitEsNamesMap.containsKey(this.esUnit)) {
                    if (this.mapping == null) {
                        throw new BizException("没有初始化mapping");
                    }

                    esUnitEsNamesMap.put(this.esUnit, new HashSet<>());
                    esUnitMappingMap.put(this.esUnit, this.mapping);
                }

                if (!preInit) {
                    this.initIndex(esUnitMappingMap.get(this.esUnit));
                }

                esUnitEsNamesMap.get(this.esUnit).add(this.esName);
            }

            return this.esName;
        }

        public void deleteIndex() {
            synchronized (this.getLock().intern()) {
                if (this.isInitialized()) {
                    esUnitEsNamesMap.get(this.esUnit).remove(this.esName);
                    if (esUnitEsNamesMap.get(this.esUnit).isEmpty()) {
                        esUnitEsNamesMap.remove(this.esUnit);
                        esUnitMappingMap.remove(this.esUnit);
                    }
                }

                SimpleSearchHelper.deleteIndex(this.esUnit.getJestClient(), this.esName);
            }
        }

        private String getLock() {
            return String.format("%s_createEsLock", this.esUnit.getBaseEsName());
        }

        private boolean isInitialized() {
            Set<String> esNames = esUnitEsNamesMap.get(this.esUnit);
            if (esNames == null) {
                return false;
            }

            if (!esNames.contains(this.esName)) {
                return false;
            }

            return true;
        }

        private void initIndex(String mapping) {
            int failTime = 0;
            while (true) {
                try {
                    IndicesExists existRequest = new IndicesExists.Builder(this.esName).build();
                    boolean exist = this.esUnit.getJestClient().execute(existRequest).isSucceeded();
                    if (!exist) {
                        CreateIndex buildRequest = new CreateIndex.Builder(this.esName).mappings(mapping).build();
                        JestResult result = this.esUnit.getJestClient().execute(buildRequest);
                        if (!result.isSucceeded()) {
                            throw new BizException(String.format("es创建 %s 索引失败: %s", this.esName, result.getErrorMessage()));
                        }

                        log.info("es创建 {} 索引成功", this.esName);
                    } else {
                        PutMapping putMappingRequest = new PutMapping.Builder(this.esName, null, mapping).build();
                        JestResult result = this.esUnit.getJestClient().execute(putMappingRequest);
                        if (!result.isSucceeded()) {
                            throw new BizException(String.format("es更新 %s mapping失败: %s", this.esName, result.getErrorMessage()));
                        }

                        log.info("es更新 {} mapping成功", this.esName);
                    }

                    break;
                } catch (Exception e) {
                    failTime++;
                    if (failTime > 1) {
                        throw new BizException(e);
                    }
                }
            }
        }
    }


    @Setter
    @Getter
    private static class EsUnit {
        private JestClient jestClient;

        private String baseEsName;

        private EsUnit() {

        }

        private static EsUnit of(JestClient jestClient, String baseEsName) {
            EsUnit data = new EsUnit();

            data.setJestClient(jestClient);
            data.setBaseEsName(baseEsName.toLowerCase());

            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EsUnit esUnit = (EsUnit) o;
            return Objects.equals(jestClient, esUnit.jestClient) && Objects.equals(baseEsName, esUnit.baseEsName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jestClient, baseEsName);
        }
    }
}
