package io.github.chaogeoop.base.business.elasticsearch;

import com.google.common.collect.Sets;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.mongodb.ISplitCollection;
import io.github.chaogeoop.base.business.mongodb.SplitCollectionHelper;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.searchbox.client.JestClient;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.search.join.ScoreMode;
import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.elasticsearch.index.query.*;
import org.springframework.data.mongodb.core.query.Query;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EsHelper {
    private static final List<String> rangeOperators = Lists.newArrayList("$gt", "$gte", "$lt", "$lte");

    private static final List<String> regexOperators = Lists.newArrayList("$regex", "$options");

    private static final ConcurrentHashMap<Class<? extends BaseEs>, Map<String, Object>> esMappingMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Class<? extends BaseEs>, TextAndNestedInfo> esTextAndNestedInfoMap = new ConcurrentHashMap<>();

    private static final Map<String, Map<String, String>> textKeywordField = Map.of("keyword", Map.of("type", "keyword"));

    public static <K extends ISearch<? extends BaseEs>> String getBaseEsName(K data) {
        Class<? extends BaseEs> esClazz = data.giveEsModel();

        if (esClazz.isAnnotationPresent(EsTableName.class)) {
            return esClazz.getAnnotation(EsTableName.class).value();
        }

        if (BaseModel.class.isAssignableFrom(data.getClass())) {
            return BaseModel.getBaseCollectionNameByClazz((Class<? extends BaseModel>) data.getClass());
        }

        throw new BizException("数据非法");
    }

    public static BoolQueryBuilder convert(Query query, TextAndNestedInfo textAndNestedInfo, boolean needBoost) {
        return convert(query.getQueryObject(), textAndNestedInfo, needBoost);
    }

    public static BoolQueryBuilder convert(Document document, TextAndNestedInfo textAndNestedInfo, boolean needBoost) {
        ConvertEntity entity = new ConvertEntity(document, textAndNestedInfo, needBoost);

        return entity.convert();
    }

    private static class ConvertEntity {
        private final Document document;
        private final boolean needBoost;
        private final Map<String, String> nestedFieldPathMap;
        private final Set<String> hasKeywordTextFields;

        ConvertEntity(Document document, TextAndNestedInfo textAndNestedInfo, boolean needBoost) {
            this.document = document;
            this.needBoost = needBoost;
            this.nestedFieldPathMap = textAndNestedInfo.giveNestedFiledPathMap();
            this.hasKeywordTextFields = textAndNestedInfo.getHasKeywordTextFields();
        }

        public BoolQueryBuilder convert() {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

            for (Map.Entry<String, Object> entry : this.document.entrySet()) {
                convertBranch(entry.getKey(), entry.getValue(), boolQueryBuilder, false);
            }

            return boolQueryBuilder;
        }

        private void convertBranch(String key, Object branch, BoolQueryBuilder branchBoolQuery, boolean noConvertNested) {
            if (branch instanceof Collection) {
                if ("$or".equals(key)) {
                    branchBoolQuery.minimumShouldMatch(1);
                }

                for (Document obj : ((Collection<Document>) branch)) {
                    BoolQueryBuilder subBoolQuery = new BoolQueryBuilder();

                    if ("$or".equals(key)) {
                        branchBoolQuery.should(subBoolQuery);
                    } else if ("$nor".equals(key)) {
                        branchBoolQuery.mustNot(subBoolQuery);
                    } else if ("$and".equals(key)) {
                        this.mustOrFilter(branchBoolQuery, subBoolQuery);
                    } else {
                        throw new BizException("不支持这个操作符");
                    }

                    for (Map.Entry<String, Object> entry : obj.entrySet()) {
                        convertBranch(entry.getKey(), entry.getValue(), subBoolQuery, noConvertNested);
                    }
                }
                return;
            }

            if (branch instanceof BsonRegularExpression) {
                RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder(this.getTermField(key), ((BsonRegularExpression) branch).getPattern().replaceAll("^\\^|\\$$", ""));

                String options = ((BsonRegularExpression) branch).getOptions();
                if (options != null && options.contains("i")) {
                    regexpQueryBuilder.caseInsensitive(true);
                }

                mustAfterJudgeNested(key, branchBoolQuery, regexpQueryBuilder, noConvertNested);
                return;
            }

            if (!(branch instanceof Document)) {
                mustAfterJudgeNested(key, branchBoolQuery, convertValueToQuery(key, branch), noConvertNested);
                return;
            }

            Map<String, Object> rangeDocument = new HashMap<>();
            Map<String, Object> regexDocument = new HashMap<>();


            Iterator<Map.Entry<String, Object>> iterator = ((Document) branch).entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();

                if (rangeOperators.contains(entry.getKey())) {
                    rangeDocument.put(entry.getKey(), entry.getValue());
                    iterator.remove();
                } else if (regexOperators.contains(entry.getKey())) {
                    regexDocument.put(entry.getKey(), entry.getValue());
                    iterator.remove();
                }
            }

            if (!rangeDocument.isEmpty()) {
                RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(key);

                for (Map.Entry<String, Object> entry : rangeDocument.entrySet()) {
                    if ("$gt".equals(entry.getKey())) {
                        rangeQueryBuilder.gt(entry.getValue());
                    }

                    if ("$gte".equals(entry.getKey())) {
                        rangeQueryBuilder.gte(entry.getValue());
                    }

                    if ("$lt".equals(entry.getKey())) {
                        rangeQueryBuilder.lt(entry.getValue());
                    }

                    if ("$lte".equals(entry.getKey())) {
                        rangeQueryBuilder.lte(entry.getValue());
                    }
                }

                mustAfterJudgeNested(key, branchBoolQuery, rangeQueryBuilder, noConvertNested);
            }

            if (!regexDocument.isEmpty()) {
                RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder(this.getTermField(key), ((String) regexDocument.get("$regex")).replaceAll("^\\^|\\$$", ""));
                if (regexDocument.get("$options") instanceof String && ((String) regexDocument.get("$options")).contains("i")) {
                    regexpQueryBuilder.caseInsensitive(true);
                }

                mustAfterJudgeNested(key, branchBoolQuery, regexpQueryBuilder, noConvertNested);
            }


            for (Map.Entry<String, Object> entry : ((Document) branch).entrySet()) {
                if (!entry.getKey().startsWith("$")) {
                    convertBranch(key + "." + entry.getKey(), entry.getValue(), branchBoolQuery, noConvertNested);
                    continue;
                }

                if ("$exists".equals(entry.getKey())) {
                    if (Boolean.TRUE.equals(entry.getValue())) {
                        mustAfterJudgeNested(key, branchBoolQuery, new ExistsQueryBuilder(key), noConvertNested);
                    } else {
                        mustNotAfterJudgeNested(key, branchBoolQuery, new ExistsQueryBuilder(key), noConvertNested);
                    }

                    continue;
                }

                if ("$elemMatch".equals(entry.getKey())) {
                    BoolQueryBuilder subBoolQuery = new BoolQueryBuilder();

                    Document innerDocument = (Document) entry.getValue();

                    for (Map.Entry<String, Object> innerEntry : innerDocument.entrySet()) {
                        String nestedKey = key + "." + innerEntry.getKey();

                        convertBranch(nestedKey, innerEntry.getValue(), subBoolQuery, true);
                    }

                    this.mustOrFilter(branchBoolQuery, new NestedQueryBuilder(key, subBoolQuery, ScoreMode.None));

                    continue;
                }

                if ("$not".equals(entry.getKey())) {
                    BoolQueryBuilder subBoolQuery = new BoolQueryBuilder();
                    convertBranch(key, entry.getValue(), subBoolQuery, noConvertNested);

                    mustNotAfterJudgeNested(key, branchBoolQuery, subBoolQuery, noConvertNested);

                    continue;
                }

                if ("$ne".equals(entry.getKey())) {
                    mustNotAfterJudgeNested(key, branchBoolQuery, this.convertValueToQuery(key, entry.getValue()), noConvertNested);
                    continue;
                }

                if ("$eq".equals(entry.getKey())) {
                    mustAfterJudgeNested(key, branchBoolQuery, this.convertValueToQuery(key, entry.getValue()), noConvertNested);
                    continue;
                }

                if ("$nin".equals(entry.getKey())) {
                    mustNotAfterJudgeNested(key, branchBoolQuery, this.getInNinSubBoolQuery(key, (Collection<Object>) entry.getValue()), noConvertNested);
                    continue;
                }

                if ("$in".equals(entry.getKey())) {
                    mustAfterJudgeNested(key, branchBoolQuery, this.getInNinSubBoolQuery(key, (Collection<Object>) entry.getValue()), noConvertNested);
                    continue;
                }

                if ("$all".equals(entry.getKey())) {
                    mustAfterJudgeNested(key, branchBoolQuery, this.getAllSubBoolQuery(key, (Collection<Object>) entry.getValue()), noConvertNested);
                    continue;
                }

                throw new BizException("暂不支持这个操作符");
            }
        }


        private void mustOrFilter(BoolQueryBuilder boolQuery, QueryBuilder query) {
            if (!this.needBoost) {
                boolQuery.filter(query);
            } else {
                boolQuery.must(query);
            }
        }

        private void mustAfterJudgeNested(String key, BoolQueryBuilder branchBoolQuery, QueryBuilder queryBuilder, boolean noConvertNested) {
            if (noConvertNested) {
                this.mustOrFilter(branchBoolQuery, queryBuilder);
                return;
            }

            if (this.nestedFieldPathMap.containsKey(key)) {
                this.mustOrFilter(branchBoolQuery, new NestedQueryBuilder(this.nestedFieldPathMap.get(key), queryBuilder, ScoreMode.None));
            } else {
                this.mustOrFilter(branchBoolQuery, queryBuilder);
            }
        }

        private void mustNotAfterJudgeNested(String key, BoolQueryBuilder branchBoolQuery, QueryBuilder queryBuilder, boolean noConvertNested) {
            if (noConvertNested) {
                branchBoolQuery.mustNot(queryBuilder);
                return;
            }

            if (this.nestedFieldPathMap.containsKey(key)) {
                branchBoolQuery.mustNot(new NestedQueryBuilder(this.nestedFieldPathMap.get(key), queryBuilder, ScoreMode.None));
            } else {
                branchBoolQuery.mustNot(queryBuilder);
            }
        }

        private String getTermField(String key) {
            if (this.hasKeywordTextFields.contains(key)) {
                return key + ".keyword";
            }

            return key;
        }

        private QueryBuilder convertValueToQuery(String key, Object value) {
            QueryBuilder queryBuilder;

            if (value == null) {
                BoolQueryBuilder subBoolQuery = new BoolQueryBuilder();
                ExistsQueryBuilder existsQueryBuilder = new ExistsQueryBuilder(key);
                subBoolQuery.mustNot(existsQueryBuilder);

                queryBuilder = subBoolQuery;
            } else {
                queryBuilder = new TermQueryBuilder(this.getTermField(key), value);
            }

            return queryBuilder;
        }

        private BoolQueryBuilder getInNinSubBoolQuery(String key, Collection<Object> list) {
            BoolQueryBuilder subBoolQuery = new BoolQueryBuilder().minimumShouldMatch(1);

            ListNullJudge listNullJudge = ListNullJudge.of(list);
            if (Boolean.TRUE.equals(listNullJudge.getHasNull())) {
                subBoolQuery.should(this.convertValueToQuery(key, null));
            }

            if (!listNullJudge.getNotNullList().isEmpty()) {
                subBoolQuery.should(new TermsQueryBuilder(this.getTermField(key), listNullJudge.getNotNullList()));
            }

            return subBoolQuery;
        }

        private BoolQueryBuilder getAllSubBoolQuery(String key, Collection<Object> list) {
            BoolQueryBuilder subBoolQuery = new BoolQueryBuilder();

            ListNullJudge listNullJudge = ListNullJudge.of(list);
            if (Boolean.TRUE.equals(listNullJudge.getHasNull())) {
                this.mustOrFilter(subBoolQuery, this.convertValueToQuery(key, null));
            }

            for (Object data : listNullJudge.getNotNullList()) {
                this.mustOrFilter(subBoolQuery, new TermQueryBuilder(this.getTermField(key), data));
            }

            return subBoolQuery;
        }
    }

    public static Map<String, Object> getMapping(Class<? extends BaseEs> clazz) {
        Map<String, Object> mapping = esMappingMap.get(clazz);
        if (mapping != null) {
            return Maps.newHashMap(mapping);
        }

        EsClazzEntity entity = new EsClazzEntity(clazz);
        entity.initCache();

        return Maps.newHashMap(esMappingMap.get(clazz));
    }

    public static TextAndNestedInfo getTextAndNestedInfo(Class<? extends BaseEs> clazz) {
        TextAndNestedInfo textAndNestedInfo = esTextAndNestedInfoMap.get(clazz);
        if (textAndNestedInfo != null) {
            return textAndNestedInfo.giveCopy();
        }

        EsClazzEntity entity = new EsClazzEntity(clazz);
        entity.initCache();

        return esTextAndNestedInfoMap.get(clazz).giveCopy();
    }

    @Setter
    @Getter
    private static class ListNullJudge {
        private Boolean hasNull = false;

        private List<Object> notNullList = new ArrayList<>();

        public static ListNullJudge of(Collection<Object> list) {
            if (list.isEmpty()) {
                throw new BizException("不能查询空数组");
            }

            ListNullJudge data = new ListNullJudge();

            for (Object obj : list) {
                if (obj == null) {
                    data.setHasNull(true);
                    continue;
                }

                data.getNotNullList().add(obj);
            }

            return data;
        }
    }

    public static class EsClazzEntity {
        private final Class<? extends BaseEs> clazz;
        private final FieldNode root;
        private final Set<FieldNode> tailNodes = new HashSet<>();
        private final Set<FieldNode> tailTextNodes = new HashSet<>();
        private final Set<FieldNode> tailTermNodes = new HashSet<>();


        public EsClazzEntity(Class<? extends BaseEs> inputClazz) {
            this.clazz = inputClazz;
            this.root = FieldNode.of(null, null);
            this.buildTree(this.root, inputClazz);

            for (FieldNode tailNode : this.tailNodes) {
                if (EsTypeEnum.TEXT.equals(tailNode.getDetail().getEsField().type())) {
                    tailTextNodes.add(tailNode);
                    continue;
                }

                tailTermNodes.add(tailNode);
            }
        }

        public void initCache() {
            if (!esMappingMap.containsKey(this.clazz)) {
                esMappingMap.put(this.clazz, this.getMapping());
            }

            if (!esTextAndNestedInfoMap.containsKey(this.clazz)) {
                esTextAndNestedInfoMap.put(this.clazz, this.getTextAndNestedInfo());
            }
        }

        public Map<String, Object> getMapping() {
            return this.getMapping(this.root);
        }

        private Map<String, Object> getMapping(FieldNode node) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> temp = new LinkedHashMap<>();
            result.put("properties", temp);

            for (FieldNode child : node.getChildren()) {
                EsTypeEnum esType = child.getDetail().getEsField().type();

                Map<String, Object> subTemp = new HashMap<>();
                subTemp.put("type", esType.getEsType());

                if (EsTypeEnum.OBJECT.equals(esType)) {
                    subTemp.putAll(this.getMapping(child));
                }

                if (EsTypeEnum.NESTED.equals(esType)) {
                    subTemp.putAll(this.getMapping(child));
                }

                if (EsTypeEnum.TEXT.equals(esType) && child.getDetail().getEsField().textHasKeywordField()) {
                    subTemp.put("fields", textKeywordField);
                }

                temp.put(child.getDetail().getFieldName(), subTemp);
            }

            return result;
        }

        public TextAndNestedInfo getTextAndNestedInfo() {
            TextAndNestedInfo data = new TextAndNestedInfo();

            data.setNestedTermFieldPathMap(this.getNestedTermFieldPathMap());

            for (FieldNode node : this.tailTextNodes) {
                boolean textHasKeywordField = node.getDetail().getEsField().textHasKeywordField();

                List<FieldNode> list = this.buildDescNodeChain(node);
                if (list.size() == 1) {
                    String fieldName = list.get(0).getDetail().getFieldName();

                    data.getTextFields().add(fieldName);
                    if (textHasKeywordField) {
                        data.getHasKeywordTextFields().add(fieldName);
                    }
                    continue;
                }

                Collections.reverse(list);

                String withPointField = this.getWithPointField(list);
                if (textHasKeywordField) {
                    data.getHasKeywordTextFields().add(withPointField);
                }

                List<FieldNode> nestedNodes = CollectionHelper.find(list, o -> EsTypeEnum.NESTED.equals(o.getDetail().getEsField().type()));
                if (nestedNodes.isEmpty()) {
                    data.getTextFields().add(withPointField);
                    continue;
                }

                String path = this.getPath(list);
                data.getNestedTextFieldPathMap().put(withPointField, path);
            }


            return data;
        }

        private Map<String, String> getNestedTermFieldPathMap() {
            Map<String, String> map = new HashMap<>();

            for (FieldNode node : this.tailTermNodes) {
                List<FieldNode> list = this.buildDescNodeChain(node);
                if (list.size() == 1) {
                    continue;
                }

                List<FieldNode> nestedNodes = CollectionHelper.find(list, o -> EsTypeEnum.NESTED.equals(o.getDetail().getEsField().type()));
                if (nestedNodes.isEmpty()) {
                    continue;
                }

                Collections.reverse(list);

                String nestedField = this.getWithPointField(list);
                String path = this.getPath(list);

                map.put(nestedField, path);
            }

            return map;
        }

        private List<FieldNode> buildDescNodeChain(FieldNode node) {
            List<FieldNode> list = new ArrayList<>();
            list.add(node);

            FieldNode parentNode = node.getParentNode();
            while (parentNode != this.root) {
                list.add(parentNode);
                parentNode = parentNode.getParentNode();
            }

            return list;
        }

        private String getWithPointField(List<FieldNode> list) {
            List<String> fieldNames = CollectionHelper.map(list, o -> o.getDetail().getFieldName());
            return String.join(".", fieldNames);
        }

        private String getPath(List<FieldNode> hasNestedList) {
            StringBuilder pathBuilder = new StringBuilder();
            for (FieldNode tmp : hasNestedList) {
                pathBuilder.append(tmp.getDetail().getFieldName());
                pathBuilder.append(".");
                if (EsTypeEnum.NESTED.equals(tmp.getDetail().getEsField().type())) {
                    break;
                }
            }
            pathBuilder.deleteCharAt(pathBuilder.length() - 1);

            return pathBuilder.toString();
        }

        private void buildTree(FieldNode parent, Class<?> clazz) {
            this.tailNodes.remove(parent);
            for (Field field : clazz.getDeclaredFields()) {
                FieldDetail detail = FieldDetail.of(field);

                FieldNode sonNode = parent.addChildren(detail);
                if (sonNode == null) {
                    continue;
                }

                this.tailNodes.add(sonNode);
                if (EsTypeEnum.HAS_OBJECT_TYPES.contains(detail.getEsField().type())) {
                    this.buildTree(sonNode, detail.getEsField().objectType());
                }
            }
        }
    }

    @Setter
    @Getter
    public static class TextAndNestedInfo {
        private Set<String> textFields = new HashSet<>();

        private Map<String, String> nestedTextFieldPathMap = new HashMap<>();

        private Set<String> hasKeywordTextFields = new HashSet<>();

        private Map<String, String> nestedTermFieldPathMap = new HashMap<>();


        public TextAndNestedInfo giveCopy() {
            TextAndNestedInfo data = new TextAndNestedInfo();

            data.setTextFields(Sets.newHashSet(this.textFields));
            data.setNestedTextFieldPathMap(Maps.newHashMap(this.nestedTextFieldPathMap));
            data.setHasKeywordTextFields(Sets.newHashSet(this.hasKeywordTextFields));
            data.setNestedTermFieldPathMap(Maps.newHashMap(this.nestedTermFieldPathMap));

            return data;
        }

        public Map<String, String> giveNestedFiledPathMap() {
            Map<String, String> map = new HashMap<>();

            map.putAll(this.nestedTextFieldPathMap);
            map.putAll(this.nestedTermFieldPathMap);

            return map;
        }
    }

    @Setter
    @Getter
    public static class FieldDetail {
        private String fieldName;

        private EsField esField;

        public static FieldDetail of(Field field) {
            boolean hasEsField = field.isAnnotationPresent(EsField.class);
            if (!hasEsField) {
                throw new BizException("有字段没注解EsField");
            }

            FieldDetail data = new FieldDetail();

            EsField esField = field.getAnnotation(EsField.class);

            data.setFieldName(field.getName());
            data.setEsField(esField);

            return data;
        }
    }

    @Setter
    @Getter
    public static class FieldNode {
        private Map<Class<?>, Integer> chainClazzTimesMap = new HashMap<>();

        private FieldNode parentNode;

        private FieldDetail detail;

        private List<FieldNode> children = new ArrayList<>();

        public @Nullable FieldNode addChildren(FieldDetail detail) {
            Map<Class<?>, Integer> newChainMap = Maps.newHashMap(this.chainClazzTimesMap);
            Integer beforeValue = newChainMap.get(detail.getEsField().objectType());
            if (beforeValue == null) {
                beforeValue = 0;
            }

            if (beforeValue >= 2) {
                return null;
            }

            newChainMap.put(detail.getEsField().objectType(), beforeValue + 1);

            FieldNode sonNode = of(this, detail);
            sonNode.setChainClazzTimesMap(newChainMap);

            this.children.add(sonNode);

            return sonNode;
        }

        public static FieldNode of(@Nullable FieldNode parentNode, @Nullable FieldDetail detail) {
            FieldNode data = new FieldNode();

            data.setParentNode(parentNode);
            data.setDetail(detail);

            return data;
        }
    }

    @Getter
    @Setter
    public static class SearchInput {
        private Query query;

        private EsHelper.TextAndNestedInfo textAndNestedInfo;

        private BoolQueryBuilder wordSearchQueryBuilder;

        public BoolQueryBuilder convertToEsQuery() {
            BoolQueryBuilder mainQuery = convert(this.query, this.textAndNestedInfo, false);
            if (this.wordSearchQueryBuilder != null) {
                mainQuery.must(this.wordSearchQueryBuilder);
            }

            return mainQuery;
        }

        public static SearchInput of(Query query, String word, Class<? extends BaseEs> clazz) {
            TextAndNestedInfo textAndNestedInfo = EsHelper.getTextAndNestedInfo(clazz);

            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().minimumShouldMatch(1);

            if (!textAndNestedInfo.getTextFields().isEmpty()) {
                String[] fields = textAndNestedInfo.getTextFields().toArray(new String[0]);

                boolQueryBuilder.should(new MultiMatchQueryBuilder(word, fields).minimumShouldMatch("3<75%"));
            }

            for (Map.Entry<String, String> entry : textAndNestedInfo.getNestedTextFieldPathMap().entrySet()) {
                MatchQueryBuilder temp = new MatchQueryBuilder(entry.getKey(), word).minimumShouldMatch("3<75%");

                boolQueryBuilder.should(new NestedQueryBuilder(entry.getValue(), temp, ScoreMode.None));
            }

            SearchInput searchInput = new SearchInput();
            searchInput.setQuery(query);
            searchInput.setTextAndNestedInfo(textAndNestedInfo);
            searchInput.setWordSearchQueryBuilder(boolQueryBuilder);

            return searchInput;
        }
    }

    private static class EsInfoEntity<K extends ISearch<? extends BaseEs>> {
        private final K data;
        private final String baseEsName;
        private final String accordEsName;

        EsInfoEntity(K data) {
            this.data = data;
            this.baseEsName = EsHelper.getBaseEsName(this.data);
            this.accordEsName = this.calAccordEsName();
        }

        private String calAccordEsName() {
            Class<? extends BaseEs> esClazz = this.data.giveEsModel();

            if (esClazz.isAnnotationPresent(EsTableName.class)) {
                return esClazz.getAnnotation(EsTableName.class).value();
            }

            if (!(this.data instanceof ISplitCollection)) {
                return this.baseEsName;
            }

            String splitIndex = ((ISplitCollection) this.data).calSplitIndex();

            return SplitCollectionHelper.combineNameWithSplitIndex(this.baseEsName, splitIndex);
        }

        private String getMapping(JestClient jestClient) {
            String mapping = BaseEs.getMappingFromCache(jestClient, this.baseEsName);
            if (mapping != null) {
                return mapping;
            }

            mapping = JsonHelper.writeValueAsString(EsHelper.getMapping(this.data.giveEsModel()));

            BaseEs.getAccordEsNameByData(jestClient, this.baseEsName, this.accordEsName, mapping);

            return mapping;
        }

        public EsUnitInfo getInfo(JestClient jestClient) {
            EsUnitInfo info = new EsUnitInfo();

            info.setBaseEsName(this.baseEsName);
            info.setEsName(this.accordEsName);
            info.setMapping(this.getMapping(jestClient));

            return info;
        }
    }

    @Setter
    @Getter
    public static class EsUnitInfo {
        private String baseEsName;

        private String esName;

        private String mapping;

        public static <M extends ISearch<? extends BaseEs>> EsUnitInfo of(M judge, JestClient jestClient) {
            EsInfoEntity<M> entity = new EsInfoEntity<>(judge);

            return entity.getInfo(jestClient);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EsUnitInfo info = (EsUnitInfo) o;
            return Objects.equals(baseEsName, info.baseEsName) && Objects.equals(esName, info.esName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseEsName, esName);
        }
    }

    @Setter
    @Getter
    public static class InitEsUnit {
        private String baseEsName;

        private Class<? extends BaseEs> esClazz;

        private String mapping;

        public static InitEsUnit of(Class<? extends ISearch<? extends BaseEs>> clazz) {
            InitEsUnit result = new InitEsUnit();

            ISearch<? extends BaseEs> data;
            try {
                data = clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new BizException("创建数据错误");
            }

            result.setBaseEsName(EsHelper.getBaseEsName(data));
            result.setEsClazz(data.giveEsModel());
            result.setMapping(JsonHelper.writeValueAsString(EsHelper.getMapping(data.giveEsModel())));

            return result;
        }
    }
}
