package io.github.chaogeoop.base.example.repository.domains;

import com.google.common.collect.Lists;
import io.github.chaogeoop.base.business.elasticsearch.ISearch;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.mongodb.ISplitCollection;
import io.github.chaogeoop.base.example.repository.es.EsTestInEs;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Setter
@Getter
@Document("estests")
public class EsTest extends BaseModel implements ISplitCollection, ISearch<EsTestInEs> {
    @Indexed
    private Long familyId;

    @Indexed(unique = true)
    private Long uid;

    private String name;

    private List<Long> tagIds;

    private Address address;

    private List<Address> addressList = new ArrayList<>();

    private Date created = new Date();

    @Override
    public String calSplitIndex() {
        int offset = Math.abs(this.getFamilyId().toString().hashCode() % 2);

        return String.valueOf(offset);
    }

    @Override
    public Class<EsTestInEs> giveEsModel() {
        return EsTestInEs.class;
    }

    @Override
    public EsTestInEs giveEsData() {
        EsTestInEs esData = new EsTestInEs();

        esData.setFamilyId(this.familyId);
        esData.setUid(this.uid);
        esData.setName(this.name);
        esData.setTagIds(this.tagIds);

        EsTestInEs.Child child = new EsTestInEs.Child();
        child.setName("儿子");
        child.setNickname("崽崽");

        EsTestInEs.Outer outer = new EsTestInEs.Outer();
        outer.setChildren(Lists.newArrayList(child));

        EsTestInEs.Father father = new EsTestInEs.Father();
        father.setName("父亲");
        father.setNickname("爸爸");
        father.setOuter(outer);

        esData.setFathers(Lists.newArrayList(father));

        return esData;
    }

    @Setter
    @Getter
    public static class Address {
        private String country;

        private String province;
    }

    public static EsTest splitKeyOf(Long familyId) {
        EsTest data = new EsTest();

        data.setFamilyId(familyId);

        return data;
    }
}
