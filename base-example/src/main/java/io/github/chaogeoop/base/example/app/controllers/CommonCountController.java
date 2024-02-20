package io.github.chaogeoop.base.example.app.controllers;

import io.github.chaogeoop.base.business.common.CommonCountProvider;
import io.github.chaogeoop.base.business.mongodb.MongoPersistEntity;
import io.github.chaogeoop.base.business.mongodb.PersistProvider;
import io.github.chaogeoop.base.business.redis.DistributedKeyProvider;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.entities.HttpResult;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.common.helpers.DateHelper;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.keyregisters.CommonCountKeyRegister;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/commonCount")
public class CommonCountController {
    @Autowired
    private CommonCountProvider commonCountProvider;

    @Autowired
    private PersistProvider persistProvider;

    @Autowired
    private RedisProvider redisProvider;

    @PostMapping("/upload")
    public HttpResult<Boolean> upload(@RequestBody BizInput input) {
        Random rand = new Random();

        List<Map<CommonCountProvider.CountBizDate, Long>> bizDateIncMapList = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            int inc = rand.nextInt(10);

            Map<CommonCountProvider.CountBizDate, Long> map = new HashMap<>();
            for (String date : input.getDates()) {
                for (CommonCountProvider.CountBiz biz : input.getBizList()) {
                    CommonCountProvider.CountBizDate bizDate = biz.convertToBizDate(date);

                    map.put(bizDate, (long) inc);
                }
            }

            bizDateIncMapList.add(map);
        }

        Collections.shuffle(bizDateIncMapList);

        MongoPersistEntity.PersistEntity persistEntity = this.commonCountProvider.insertPersistHistory(bizDateIncMapList);

        this.persistProvider.persist(Lists.newArrayList(persistEntity));

        return HttpResult.of(true);
    }

    @PostMapping("/getTotalMap")
    public HttpResult<Map<String, Long>> getTotalMap(@RequestBody BizInput input) {
        Map<String, Long> result = new HashMap<>();

        Map<CommonCountProvider.CountBiz, Long> map = this.commonCountProvider.getBizTotalMap(Sets.newHashSet(input.getBizList()));
        for (Map.Entry<CommonCountProvider.CountBiz, Long> entry : map.entrySet()) {
            result.put(JsonHelper.writeValueAsString(entry.getKey()), entry.getValue());
        }

        return HttpResult.of(result);
    }

    @PostMapping("/getDateMap")
    public HttpResult<Map<String, Map<String, Long>>> getDateMap(@RequestBody BizInput input) {
        Map<String, Map<String, Long>> result = new HashMap<>();

        Map<CommonCountProvider.CountBiz, Map<String, Long>> map = this.commonCountProvider.getBizLogsMap(Sets.newHashSet(input.getBizList()),
                input.getDates()
        );

        for (Map.Entry<CommonCountProvider.CountBiz, Map<String, Long>> entry : map.entrySet()) {
            result.put(JsonHelper.writeValueAsString(entry.getKey()), entry.getValue());
        }

        return HttpResult.of(result);
    }

    @PostMapping("/getCacheExistMap")
    public HttpResult<Map<String, Map<String, Boolean>>> getCacheExistMap(@RequestBody BizInput input) {
        Map<String, Map<String, Boolean>> map = new HashMap<>();

        List<String> dates = CollectionHelper.unique(input.getDates());
        dates.sort(Comparator.comparing(o -> DateHelper.parseStringDate(o, DateHelper.DateFormatEnum.fullUntilDay)));

        for (CommonCountProvider.CountBiz biz : input.getBizList()) {
            map.put(JsonHelper.writeValueAsString(biz), new LinkedHashMap<>());
            for (String date : dates) {
                CommonCountProvider.CountBizDate bizDate = biz.convertToBizDate(date);

                DistributedKeyProvider.KeyEntity<CommonCountKeyRegister.CommonCountDistributedKey> keyEntity = DistributedKeyProvider.KeyEntity.of(
                        CommonCountKeyRegister.COUNT_BIZ_DATE_CACHE_TYPE,
                        JsonHelper.writeValueAsString(bizDate)
                );

                boolean exists = this.redisProvider.exists(keyEntity);

                map.get(JsonHelper.writeValueAsString(biz)).put(date, exists);
            }
        }


        return HttpResult.of(map);
    }

    @GetMapping("/freezeColdData")
    public HttpResult<Boolean> freezeColdData() {
        this.commonCountProvider.freezeColdData(3);

        return HttpResult.of(true);
    }

    @Setter
    @Getter
    public static class BizInput {
        private List<CommonCountProvider.CountBiz> bizList = new ArrayList<>();

        private List<String> dates = new ArrayList<>();
    }
}
