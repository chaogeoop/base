package io.github.chaogeoop.base.example.repository.domains;

import io.github.chaogeoop.base.business.common.helpers.JsonHelper;
import io.github.chaogeoop.base.business.elasticsearch.EsField;
import io.github.chaogeoop.base.business.elasticsearch.EsTypeEnum;
import io.github.chaogeoop.base.business.elasticsearch.IBaseEs;
import io.github.chaogeoop.base.business.elasticsearch.ISearch;
import io.github.chaogeoop.base.business.mongodb.BaseModel;
import io.github.chaogeoop.base.business.mongodb.ISplitCollection;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Setter
@Getter
@Document("estests")
//@EsTableName("estestines")
public class EsTest extends BaseModel implements ISplitCollection, IBaseEs, ISearch<EsTest> {
    @Indexed
    @EsField(type = EsTypeEnum.LONG)
    private Long familyId;

    @Indexed(unique = true)
    @EsField(type = EsTypeEnum.LONG)
    private Long uid;

    @EsField(type = EsTypeEnum.TEXT)
    private String name;

    @EsField(type = EsTypeEnum.LONG)
    private List<Long> tagIds;

    @EsField(type = EsTypeEnum.OBJECT, objectType = Address.class)
    private Address address;

    @EsField(type = EsTypeEnum.DATE)
    private Date created = new Date();

    @EsField(type = EsTypeEnum.NESTED, objectType = Father.class)
    private List<Father> fathers;

    @Override
    public String calSplitIndex() {
        int offset = Math.abs(this.getFamilyId().toString().hashCode() % 2);

        return String.valueOf(offset);
    }

    @Override
    public Class<EsTest> giveEsModel() {
        return EsTest.class;
    }

    @Override
    public String giveEsJson() {
        return JsonHelper.writeValueAsString(this);
    }

    @Setter
    @Getter
    public static class Address {
        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String country;

        @EsField(type = EsTypeEnum.KEYWORD)
        private String province;

        @EsField(type = EsTypeEnum.OBJECT, objectType = Address.class)
        private Address address;

        @EsField(type = EsTypeEnum.NESTED, objectType = Address.class)
        private List<Address> addressList;
    }

    @Setter
    @Getter
    public static class Father {
        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String name;

        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String nickname;

        @EsField(type = EsTypeEnum.OBJECT, objectType = Outer.class)
        private Outer outer;
    }

    @Setter
    @Getter
    public static class Outer {
        @EsField(type = EsTypeEnum.NESTED, objectType = Child.class)
        private List<Child> children;
    }

    @Setter
    @Getter
    public static class Child {
        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String name;

        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String nickname;
    }

    public static EsTest splitKeyOf(Long familyId) {
        EsTest data = new EsTest();

        data.setFamilyId(familyId);

        return data;
    }
}
