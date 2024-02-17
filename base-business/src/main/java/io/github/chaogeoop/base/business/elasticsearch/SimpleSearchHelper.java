package io.github.chaogeoop.base.business.elasticsearch;

import io.github.chaogeoop.base.business.common.entities.EsPageSplitter;
import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.entities.ListPage;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import com.google.gson.JsonElement;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.DeleteIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SimpleSearchHelper {
    public static void deleteIndex(JestClient jestClient, String esName) {
        DeleteIndex build = new DeleteIndex.Builder(esName).build();
        JestResult result = null;
        try {
            result = jestClient.execute(build);
        } catch (IOException e) {
            throw new BizException(e.getMessage());
        }
        if (!result.isSucceeded()) {
            throw new BizException(result.getErrorMessage());
        }
    }

    public static void insertOrUpdateData(JestClient jestClient, String esName, String uniqueId, String data) {
        Index build = new Index.Builder(data).index(esName).type("_doc").id(uniqueId).build();
        DocumentResult result = null;
        try {
            result = jestClient.execute(build);
        } catch (IOException e) {
            throw new BizException(e.getMessage());
        }
        if (!result.isSucceeded()) {
            throw new BizException(result.getErrorMessage());
        }
    }

    public static void deleteData(JestClient jestClient, String esName, String uniqueId) {
        Delete build = new Delete.Builder(uniqueId).index(esName).type("_doc").build();
        DocumentResult result = null;
        try {
            result = jestClient.execute(build);
        } catch (IOException e) {
            throw new BizException(e.getMessage());
        }
        if (!result.isSucceeded()) {
            throw new BizException(result.getErrorMessage());
        }
    }

    public static <M extends IBaseEs> ListPage<M> pageQuery(
            JestClient jestClient, QueryBuilder queryBuilder, EsPageSplitter esPageSplitter, Set<String> indicates, Class<M> clazz
    ) {
        if (indicates.isEmpty()) {
            return ListPage.of(esPageSplitter.getOffset(), esPageSplitter.getLimit(), 0, new ArrayList<>());
        }

        List<SortBuilder<?>> orders = esPageSplitter.getEsSort();

        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.query(queryBuilder);
        for (SortBuilder<?> order : orders) {
            searchBuilder.sort(order);
        }
        searchBuilder.from(esPageSplitter.getOffset());
        searchBuilder.size(esPageSplitter.getLimit());

        Search document = new Search.Builder(searchBuilder.toString()).addIndices(indicates).addType("_doc").build();
        SearchResult page = null;
        try {
            page = jestClient.execute(document);
        } catch (IOException e) {
            throw new BizException(e.getMessage());
        }
        if (!page.isSucceeded()) {
            throw new BizException(page.getErrorMessage());
        }

        List<M> list = new ArrayList<>();
        for (JsonElement element : page.getJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray()) {
            String stringData = element.getAsJsonObject().get("_source").getAsJsonObject().toString();
            list.add(JsonHelper.readValue(stringData, clazz));
        }

        long count = page.getJsonObject().get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsLong();

        return ListPage.of(esPageSplitter.getOffset(), esPageSplitter.getLimit(), count, list);
    }

    public static long count(JestClient jestClient, QueryBuilder queryBuilder, Set<String> indicates) {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.query(queryBuilder);

        Count document = new Count.Builder().query(searchBuilder.toString()).addIndices(indicates).addType("_doc").build();
        CountResult countResult = null;
        try {
            countResult = jestClient.execute(document);
        } catch (IOException e) {
            throw new BizException(e.getMessage());
        }
        if (!countResult.isSucceeded()) {
            throw new BizException(countResult.getErrorMessage());
        }

        Double count = countResult.getCount();
        if (count == null) {
            return 0;
        }

        return count.longValue();
    }
}
