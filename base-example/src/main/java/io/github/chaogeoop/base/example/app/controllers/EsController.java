package io.github.chaogeoop.base.example.app.controllers;

import io.github.chaogeoop.base.business.common.annotations.DatabaseAction;
import io.github.chaogeoop.base.business.common.annotations.IoMonitor;
import io.github.chaogeoop.base.business.common.annotations.UserInfo;
import io.github.chaogeoop.base.business.common.enums.DatabaseActionEnum;
import io.github.chaogeoop.base.business.mongodb.MongoPersistEntity;
import io.github.chaogeoop.base.business.mongodb.PersistProvider;
import io.github.chaogeoop.base.business.common.entities.HttpResult;
import io.github.chaogeoop.base.example.app.services.EsService;
import io.github.chaogeoop.base.example.repository.domains.EsTest;
import com.google.common.collect.Lists;
import io.github.chaogeoop.base.example.repository.entities.UserContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/es")
public class EsController {
    @Autowired
    private EsService esService;

    @Autowired
    private PersistProvider persistProvider;

    @PostMapping("/buildIndex")
    @DatabaseAction(action = DatabaseActionEnum.WRITTEN_SOON_READ)
    public HttpResult<Boolean> buildIndex(
            @UserInfo UserContext userContext,
            @RequestBody EsService.EsTestInput input
    ) {
        MongoPersistEntity.PersistEntity persistEntity = this.esService.inputAndSave(input);

        this.persistProvider.persist(Lists.newArrayList(persistEntity));

        return HttpResult.of(true);
    }

    @PostMapping("/deleteIndex")
    public HttpResult<Boolean> deleteIndex() throws IOException {
        this.esService.deleteIndexList();

        return HttpResult.of(true);
    }

    @PostMapping("/findTest")
    @IoMonitor(intervalTimes = 1)
    @DatabaseAction(action = DatabaseActionEnum.JUDGE_READ)
    public HttpResult<List<EsTest>> findTest(
            @UserInfo UserContext userContext,
            @RequestBody EsService.EsTestFinder obj
    ) throws IOException {
        List<EsTest> result = this.esService.findTest(obj);

        return HttpResult.of(result);
    }

}
