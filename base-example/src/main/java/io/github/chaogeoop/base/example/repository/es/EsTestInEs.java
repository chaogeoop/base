package io.github.chaogeoop.base.example.repository.es;

import io.github.chaogeoop.base.business.elasticsearch.IBaseEs;
import io.github.chaogeoop.base.business.elasticsearch.EsField;
import io.github.chaogeoop.base.business.elasticsearch.EsTypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@JsonPropertyOrder({"familyId", "uid", "name", "tagIds", "fathers"})
@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
//@EsTableName("estestines")
public class EsTestInEs implements IBaseEs {
    @EsField(type = EsTypeEnum.LONG)
    private Long familyId;

    @EsField(type = EsTypeEnum.LONG)
    private Long uid;

    @EsField(type = EsTypeEnum.TEXT)
    private String name;

    @EsField(type = EsTypeEnum.LONG)
    private List<Long> tagIds;

    @EsField(type = EsTypeEnum.NESTED, objectType = Father.class)
    private List<Father> fathers;

    @Setter
    @Getter
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
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
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Outer {
        @EsField(type = EsTypeEnum.NESTED, objectType = Child.class)
        private List<Child> children;
    }

    @Setter
    @Getter
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Child {
        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String name;

        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String nickname;
    }
}
