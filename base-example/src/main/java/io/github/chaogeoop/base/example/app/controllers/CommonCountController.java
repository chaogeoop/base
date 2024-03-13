package io.github.chaogeoop.base.example.app.controllers;

import io.github.chaogeoop.base.business.common.CommonCountProvider;
import io.github.chaogeoop.base.business.common.annotations.IoMonitor;
import io.github.chaogeoop.base.business.common.annotations.UserInfo;
import io.github.chaogeoop.base.business.mongodb.MongoPersistEntity;
import io.github.chaogeoop.base.business.mongodb.PersistProvider;
import io.github.chaogeoop.base.business.redis.KeyEntity;
import io.github.chaogeoop.base.business.redis.RedisProvider;
import io.github.chaogeoop.base.business.common.entities.HttpResult;
import io.github.chaogeoop.base.business.common.helpers.CollectionHelper;
import io.github.chaogeoop.base.business.common.helpers.DateHelper;
import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.example.app.keyregisters.CommonCountKeyRegister;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.github.chaogeoop.base.example.repository.entities.UserContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
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
    public HttpResult<Boolean> upload(@RequestBody BookInput input) {
        Set<CommonCountProvider.CountBiz> countBizList = CollectionHelper.map(input.getBookIds(), o -> getBookFavoriteBiz(o, input.getAction()));

        Random rand = new Random();

        List<Map<CommonCountProvider.CountBizDate, Long>> bizDateIncMapList = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            int inc = rand.nextInt(10);

            Map<CommonCountProvider.CountBizDate, Long> map = new HashMap<>();
            for (String date : input.getDates()) {
                for (CommonCountProvider.CountBiz biz : countBizList) {
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
    public HttpResult<Map<String, Long>> getTotalMap(@RequestBody BookInput input) {
        Set<CommonCountProvider.CountBiz> countBizList = CollectionHelper.map(input.getBookIds(), o -> getBookFavoriteBiz(o, input.getAction()));

        Map<String, Long> result = new HashMap<>();

        Map<CommonCountProvider.CountBiz, Long> map = this.commonCountProvider.getBizTotalMap(countBizList);
        for (Map.Entry<CommonCountProvider.CountBiz, Long> entry : map.entrySet()) {
            result.put(entry.getKey().giveJson(), entry.getValue());
        }

        return HttpResult.of(result);
    }

    @PostMapping("/getDateMap")
    public HttpResult<Map<String, Map<String, Long>>> getDateMap(@RequestBody BookInput input) {
        Set<CommonCountProvider.CountBiz> countBizList = CollectionHelper.map(input.getBookIds(), o -> getBookFavoriteBiz(o, input.getAction()));

        Map<String, Map<String, Long>> result = new HashMap<>();

        Map<CommonCountProvider.CountBiz, Map<String, Long>> map = this.commonCountProvider.getBizLogsMap(countBizList, input.getDates());

        for (Map.Entry<CommonCountProvider.CountBiz, Map<String, Long>> entry : map.entrySet()) {
            result.put(entry.getKey().giveJson(), entry.getValue());
        }

        return HttpResult.of(result);
    }

    @PostMapping("/getCacheExistMap")
    public HttpResult<Map<String, Map<String, Boolean>>> getCacheExistMap(@RequestBody BookInput input) {
        Set<CommonCountProvider.CountBiz> countBizList = CollectionHelper.map(input.getBookIds(), o -> getBookFavoriteBiz(o, input.getAction()));

        Map<String, Map<String, Boolean>> map = new HashMap<>();

        List<String> dates = CollectionHelper.unique(input.getDates());
        dates.sort(Comparator.comparing(o -> DateHelper.parseStringDate(o, DateHelper.DateFormatEnum.fullUntilDay)));

        for (CommonCountProvider.CountBiz biz : countBizList) {
            map.put(biz.giveJson(), new LinkedHashMap<>());
            for (String date : dates) {
                CommonCountProvider.CountBizDate bizDate = biz.convertToBizDate(date);

                KeyEntity<CommonCountKeyRegister.CommonCountDistributedKey> keyEntity = KeyEntity.of(
                        CommonCountKeyRegister.COUNT_BIZ_DATE_CACHE_TYPE,
                        bizDate.giveJson()
                );

                boolean exists = this.redisProvider.exists(keyEntity);

                map.get(biz.giveJson()).put(date, exists);
            }
        }


        return HttpResult.of(map);
    }

    @GetMapping("/freezeColdData")
    public HttpResult<Boolean> freezeColdData() {
        this.commonCountProvider.freezeColdData(3);

        return HttpResult.of(true);
    }


    @PostMapping("/distributeSafeInc")
    @IoMonitor(intervalTimes = 1)
    public HttpResult<Map<String, Long>> distributeSafeInc(@UserInfo UserContext userContext, @RequestBody BookInput input) {
        KeyEntity<CommonCountKeyRegister.CommonCountDistributedKey> lock = KeyEntity.of(CommonCountKeyRegister.USER_COLLECT_BOOKS_LOCK_TYPE, userContext.getUserId().toString());

        Map<String, Long> value = this.redisProvider.exeFuncWithLock(lock, m -> {
            Set<CommonCountProvider.CountBiz> userCountBizList = CollectionHelper.map(input.getBookIds(), o -> getUserBookFavoriteBiz(userContext, o, input.getAction()));
            Set<CommonCountProvider.CountBiz> pubCountBizList = CollectionHelper.map(input.getBookIds(), o -> getBookFavoriteBiz(o, input.getAction()));

            Map<String, Long> result = new HashMap<>();

            Map<CommonCountProvider.CountBiz, Long> userIncMap = new HashMap<>();
            for (CommonCountProvider.CountBiz countBiz : userCountBizList) {
                userIncMap.put(countBiz, 1L);
            }

            Date occurTime = new Date();
            if (input.getOccur() != null) {
                occurTime = DateHelper.parseStringDate(input.getOccur(), DateHelper.DateFormatEnum.fullUntilSecond);
            }
            String occurDate = DateHelper.dateToString(occurTime, DateHelper.DateFormatEnum.fullUntilDay);

            Map<CommonCountProvider.CountBizDate, Long> pubIncMap = new HashMap<>();
            for (CommonCountProvider.CountBiz countBiz : pubCountBizList) {
                pubIncMap.put(countBiz.convertToBizDate(occurDate), 1L);
            }

            Pair<MongoPersistEntity.PersistEntity, Map<CommonCountProvider.CountBiz, CommonCountProvider.CountBizEntity>> pair =
                    this.commonCountProvider.distributeSafeMultiBizCount(userIncMap, occurTime, lock);

            MongoPersistEntity.PersistEntity pubPersistEntity = this.commonCountProvider.insertPersistHistory(pubIncMap);

            this.persistProvider.persist(Lists.newArrayList(pair.getLeft(), pubPersistEntity));

            for (Map.Entry<CommonCountProvider.CountBiz, CommonCountProvider.CountBizEntity> entry : pair.getRight().entrySet()) {
                result.put(entry.getKey().giveJson(), entry.getValue().giveAfterAllTotal());
            }

            return result;
        });

        return HttpResult.of(value);
    }


    private CommonCountProvider.CountBiz getBookFavoriteBiz(Long bookId, String action) {
        CommonCountProvider.CountBiz data = new CommonCountProvider.CountBiz();

        data.setTypeId(bookId.toString());
        data.setBizType("book");
        data.setSubBizType(action);

        return data;
    }

    private CommonCountProvider.CountBiz getUserBookFavoriteBiz(UserContext userContext, Long bookId, String action) {
        CommonCountProvider.CountBiz data = new CommonCountProvider.CountBiz();

        data.setTypeId(userContext.getUserId().toString());
        data.setBizType(String.format("%sBookTimes", action));
        data.setSubBizType(bookId.toString());

        return data;
    }


    @Setter
    @Getter
    public static class BookInput {
        private Set<Long> bookIds;

        private String action;

        private List<String> dates = new ArrayList<>();

        private String occur;
    }
}
