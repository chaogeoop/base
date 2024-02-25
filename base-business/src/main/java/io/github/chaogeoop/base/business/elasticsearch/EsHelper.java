package io.github.chaogeoop.base.business.elasticsearch;

import com.google.common.collect.Sets;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.join.ScoreMode;
import org.bson.BsonRegularExpression;
import org.bson.Document;
import org.elasticsearch.index.query.*;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EsHelper {
    private static final List<String> rangeOperators = Lists.newArrayList("$gt", "$gte", "$lt", "$lte");

    private static final List<String> regexOperators = Lists.newArrayList("$regex", "$options");

    private static final ConcurrentHashMap<Class<? extends IBaseEs>, Map<String, Object>> esMappingMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Class<? extends IBaseEs>, EsFieldInfo> esFieldInfoMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Class<? extends IBaseEs>, FieldNode> esTreeMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Class<? extends ISearch<? extends IBaseEs>>, Class<? extends IBaseEs>> searchBaseEsMap = new ConcurrentHashMap<>();

    private static final Map<String, Map<String, String>> textKeywordField = Map.of("keyword", Map.of("type", "keyword"));

    public static String getBaseEsName(Class<? extends ISearch<? extends IBaseEs>> clazz) {
        Class<? extends IBaseEs> esClazz = getBaseEsClazz(clazz);

        if (esClazz.isAnnotationPresent(EsTableName.class)) {
            return esClazz.getAnnotation(EsTableName.class).value();
        }

        if (BaseModel.class.isAssignableFrom(clazz)) {
            return BaseModel.getBaseCollectionNameByClazz((Class<? extends BaseModel>) clazz);
        }

        throw new BizException("数据非法");
    }

    public static BoolQueryBuilder convert(Query query, EsFieldInfo esFieldInfo, boolean needBoost) {
        return convert(query.getQueryObject(), esFieldInfo, needBoost);
    }

    public static BoolQueryBuilder convert(Document document, EsFieldInfo esFieldInfo, boolean needBoost) {
        ConvertEntity entity = new ConvertEntity(document, esFieldInfo, needBoost);

        return entity.convert();
    }

    private static class ConvertEntity {
        private final Document document;
        private final boolean needBoost;
        private final Map<String, String> nestedFieldPathMap;
        private final Set<String> hasKeywordTextFields;

        ConvertEntity(Document document, EsFieldInfo esFieldInfo, boolean needBoost) {
            this.document = document;
            this.needBoost = needBoost;
            this.nestedFieldPathMap = esFieldInfo.giveNestedFiledPathMap();
            this.hasKeywordTextFields = esFieldInfo.getHasKeywordTextFields();
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

    public static EsFieldInfo getEsFieldInfo(Class<? extends ISearch<? extends IBaseEs>> clazz) {
        return getEsFieldInfoIntern(getBaseEsClazz(clazz));
    }

    public static String convertToJson(ISearch<? extends IBaseEs> data) {
        FieldNode tree = esTreeMap.get(data.giveEsModel());
        if (tree == null) {
            EsClazzEntity entity = new EsClazzEntity(data.giveEsModel());
            entity.initCache();
            tree = esTreeMap.get(data.giveEsModel());
        }

        LinkedHashMap<String, Object> result = tree.simplePickEsFieldFromData(data);

        return JsonHelper.writeValueAsString(result);
    }

    private static EsFieldInfo getEsFieldInfoIntern(Class<? extends IBaseEs> clazz) {
        EsFieldInfo esFieldInfo = esFieldInfoMap.get(clazz);
        if (esFieldInfo != null) {
            return esFieldInfo.giveCopy();
        }

        EsClazzEntity entity = new EsClazzEntity(clazz);
        entity.initCache();

        return esFieldInfoMap.get(clazz).giveCopy();
    }

    private static Map<String, Object> getMapping(Class<? extends IBaseEs> clazz) {
        Map<String, Object> mapping = esMappingMap.get(clazz);
        if (mapping != null) {
            return Maps.newHashMap(mapping);
        }

        EsClazzEntity entity = new EsClazzEntity(clazz);
        entity.initCache();

        return Maps.newHashMap(esMappingMap.get(clazz));
    }

    private static Class<? extends IBaseEs> getBaseEsClazz(Class<? extends ISearch<? extends IBaseEs>> searchClazz) {
        Class<? extends IBaseEs> clazz = searchBaseEsMap.get(searchClazz);
        if (clazz == null) {
            ISearch<? extends IBaseEs> data;
            try {
                data = searchClazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new BizException("创建数据错误");
            }

            searchBaseEsMap.put(searchClazz, data.giveEsModel());
            clazz = data.giveEsModel();
        }

        return clazz;
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
        private final Class<? extends IBaseEs> clazz;
        private final FieldNode root;
        private final Set<FieldNode> tailNodes = new HashSet<>();
        private final Set<FieldNode> tailTextNodes = new HashSet<>();
        private final Set<FieldNode> tailTermNodes = new HashSet<>();


        public EsClazzEntity(Class<? extends IBaseEs> inputClazz) {
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

            if (!esFieldInfoMap.containsKey(this.clazz)) {
                esFieldInfoMap.put(this.clazz, this.getEsFieldInfo());
            }

            if (!esTreeMap.containsKey(this.clazz)) {
                esTreeMap.put(this.clazz, this.root);
            }
        }

        public Map<String, Object> getMapping() {
            if (this.root.getChildren().isEmpty()) {
                throw new BizException("没有定义任何esField");
            }

            return this.getMapping(this.root);
        }

        private Map<String, Object> getMapping(FieldNode node) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> temp = new LinkedHashMap<>();
            result.put("properties", temp);
            result.put("dynamic", false);

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

                temp.put(child.getDetail().getField().getName(), subTemp);
            }

            return result;
        }

        public EsFieldInfo getEsFieldInfo() {
            EsFieldInfo data = new EsFieldInfo();

            for (FieldNode node : this.tailTermNodes) {
                List<FieldNode> list = this.buildNodeChain(node);

                Pair<String, List<String>> pair = FieldNode.getFiledNestedPathListPair(list);
                String fieldName = pair.getLeft();
                List<String> nestedPathList = pair.getRight();

                if (nestedPathList.isEmpty()) {
                    data.getTermFields().add(fieldName);
                    continue;
                }

                data.getNestedTermFieldPathMap().put(fieldName, nestedPathList.get(nestedPathList.size() - 1));
            }

            for (FieldNode node : this.tailTextNodes) {
                List<FieldNode> list = this.buildNodeChain(node);

                Pair<String, List<String>> pair = FieldNode.getFiledNestedPathListPair(list);
                String fieldName = pair.getLeft();
                List<String> nestedPathList = pair.getRight();

                if (node.getDetail().getEsField().textHasKeywordField()) {
                    data.getHasKeywordTextFields().add(fieldName);
                }

                if (nestedPathList.isEmpty()) {
                    data.getTextFields().add(fieldName);
                    continue;
                }

                data.getNestedTextFieldPathMap().put(fieldName, nestedPathList.get(nestedPathList.size() - 1));
            }

            return data;
        }

        private List<FieldNode> buildNodeChain(FieldNode tailNode) {
            List<FieldNode> list = new ArrayList<>();
            list.add(0, tailNode);

            FieldNode parentNode = tailNode.getParentNode();
            while (parentNode != this.root) {
                list.add(0, parentNode);
                parentNode = parentNode.getParentNode();
            }

            return list;
        }

        private void buildTree(FieldNode parent, Class<?> clazz) {
            this.tailNodes.remove(parent);

            Class<?> checkClazz = clazz;
            do {
                for (Field field : clazz.getDeclaredFields()) {
                    FieldDetail detail = FieldDetail.of(field);
                    if (detail == null) {
                        continue;
                    }

                    FieldNode sonNode = parent.addChildren(detail);
                    if (sonNode == null) {
                        continue;
                    }

                    this.tailNodes.add(sonNode);
                    if (EsTypeEnum.HAS_OBJECT_TYPES.contains(detail.getEsField().type())) {
                        this.buildTree(sonNode, detail.getEsField().objectType());
                        if (sonNode.getChildren().isEmpty()) {
                            parent.getChildren().remove(sonNode);
                        }
                    }
                }

                checkClazz = checkClazz.getSuperclass();
            } while (!Object.class.equals(checkClazz));

            parent.getChildren().sort(Comparator.comparing(o -> o.getDetail().getField().getName()));
        }
    }

    @Setter
    @Getter
    public static class EsFieldInfo {
        private Set<String> textFields = new HashSet<>();

        private Map<String, String> nestedTextFieldPathMap = new HashMap<>();

        private Set<String> hasKeywordTextFields = new HashSet<>();

        private Set<String> termFields = new HashSet<>();

        private Map<String, String> nestedTermFieldPathMap = new HashMap<>();


        public EsFieldInfo giveCopy() {
            EsFieldInfo data = new EsFieldInfo();

            data.setTextFields(Sets.newHashSet(this.textFields));
            data.setNestedTextFieldPathMap(Maps.newHashMap(this.nestedTextFieldPathMap));
            data.setHasKeywordTextFields(Sets.newHashSet(this.hasKeywordTextFields));
            data.setTermFields(Sets.newHashSet(this.termFields));
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
        private Field field;

        private EsField esField;

        public static @Nullable FieldDetail of(Field field) {
            boolean hasEsField = field.isAnnotationPresent(EsField.class);
            if (!hasEsField) {
                return null;
            }

            field.setAccessible(true);

            FieldDetail data = new FieldDetail();

            EsField esField = field.getAnnotation(EsField.class);

            data.setField(field);
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

        public LinkedHashMap<String, Object> simplePickEsFieldFromData(Object data) {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();

            for (FieldNode child : this.getChildren()) {
                try {
                    FieldDetail detail = child.getDetail();
                    Object fieldValue = detail.getField().get(data);
                    if (fieldValue == null) {
                        result.put(detail.getField().getName(), null);
                        continue;
                    }

                    if (EsTypeEnum.OBJECT.equals(detail.getEsField().type())) {
                        LinkedHashMap<String, Object> inner = child.simplePickEsFieldFromData(fieldValue);
                        result.put(detail.getField().getName(), inner);
                        continue;
                    }

                    if (EsTypeEnum.NESTED.equals(detail.getEsField().type())) {
                        List<LinkedHashMap<String, Object>> innerList = new ArrayList<>();
                        for (Object o : (Collection) fieldValue) {
                            LinkedHashMap<String, Object> inner = child.simplePickEsFieldFromData(o);
                            innerList.add(inner);
                        }
                        result.put(detail.getField().getName(), innerList);
                        continue;
                    }

                    result.put(detail.getField().getName(), fieldValue);
                } catch (Exception e) {
                    throw new BizException(e);
                }
            }

            return result;
        }

        public static FieldNode of(@Nullable FieldNode parentNode, @Nullable FieldDetail detail) {
            FieldNode data = new FieldNode();

            data.setParentNode(parentNode);
            data.setDetail(detail);

            return data;
        }

        public static Pair<String, List<String>> getFiledNestedPathListPair(List<FieldNode> list) {
            List<String> pathList = new ArrayList<>();

            StringBuilder fieldBuilder = new StringBuilder();
            for (FieldNode tmp : list) {
                fieldBuilder.append(tmp.getDetail().getField().getName());
                if (EsTypeEnum.NESTED.equals(tmp.getDetail().getEsField().type())) {
                    String path = fieldBuilder.toString();
                    pathList.add(path);
                }
                fieldBuilder.append(".");
            }

            String field = fieldBuilder.deleteCharAt(fieldBuilder.length() - 1).toString();

            return Pair.of(field, pathList);
        }
    }

    @Getter
    @Setter
    public static class SearchInput {
        private Query query;

        private EsFieldInfo esFieldInfo;

        private BoolQueryBuilder wordSearchQueryBuilder;

        public BoolQueryBuilder convertToEsQuery() {
            BoolQueryBuilder mainQuery = convert(this.query, this.esFieldInfo, false);
            if (this.wordSearchQueryBuilder != null) {
                mainQuery.must(this.wordSearchQueryBuilder);
            }

            return mainQuery;
        }

        public static SearchInput of(Query query, String word, Class<? extends ISearch<? extends IBaseEs>> clazz) {
            EsFieldInfo esFieldInfo = EsHelper.getEsFieldInfo(clazz);

            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().minimumShouldMatch(1);

            if (!esFieldInfo.getTextFields().isEmpty()) {
                String[] fields = esFieldInfo.getTextFields().toArray(new String[0]);

                boolQueryBuilder.should(new MultiMatchQueryBuilder(word, fields).minimumShouldMatch("3<75%"));
            }

            MultiValueMap<String, String> nestedPathTextFieldsMap = new LinkedMultiValueMap<>();
            for (Map.Entry<String, String> entry : esFieldInfo.getNestedTextFieldPathMap().entrySet()) {
                nestedPathTextFieldsMap.add(entry.getValue(), entry.getKey());
            }

            for (Map.Entry<String, List<String>> entry : nestedPathTextFieldsMap.entrySet()) {
                QueryBuilder temp;
                if (entry.getValue().size() == 1) {
                    temp = new MatchQueryBuilder(entry.getValue().get(0), word).minimumShouldMatch("3<75%");
                } else {
                    temp = new MultiMatchQueryBuilder(word, entry.getValue().toArray(new String[0])).minimumShouldMatch("3<75%");
                }

                boolQueryBuilder.should(new NestedQueryBuilder(entry.getKey(), temp, ScoreMode.None));
            }

            SearchInput searchInput = new SearchInput();
            searchInput.setQuery(query);
            searchInput.setEsFieldInfo(esFieldInfo);
            searchInput.setWordSearchQueryBuilder(boolQueryBuilder);

            return searchInput;
        }
    }

    private static class EsInfoEntity<K extends ISearch<? extends IBaseEs>> {
        private final K data;
        private final String baseEsName;
        private final String accordEsName;

        EsInfoEntity(K data) {
            this.data = data;
            this.baseEsName = EsHelper.getBaseEsName((Class<? extends ISearch<? extends IBaseEs>>) this.data.getClass());
            this.accordEsName = this.calAccordEsName();
        }

        private String calAccordEsName() {
            Class<? extends IBaseEs> esClazz = this.data.giveEsModel();

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
            String mapping = BaseEsHelper.getMappingFromCache(jestClient, this.baseEsName);
            if (mapping != null) {
                return mapping;
            }

            mapping = JsonHelper.writeValueAsString(EsHelper.getMapping(this.data.giveEsModel()));

            BaseEsHelper.getAccordEsNameByData(jestClient, this.baseEsName, this.accordEsName, mapping);

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

        public static <M extends ISearch<? extends IBaseEs>> EsUnitInfo of(M judge, JestClient jestClient) {
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

        private Class<? extends IBaseEs> esClazz;

        private String mapping;

        public static InitEsUnit of(Class<? extends ISearch<? extends IBaseEs>> clazz) {
            InitEsUnit result = new InitEsUnit();

            Class<? extends IBaseEs> baseEsClazz = EsHelper.getBaseEsClazz(clazz);

            result.setBaseEsName(EsHelper.getBaseEsName(clazz));
            result.setEsClazz(baseEsClazz);
            result.setMapping(JsonHelper.writeValueAsString(EsHelper.getMapping(baseEsClazz)));

            return result;
        }
    }
}
