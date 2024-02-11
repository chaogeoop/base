package io.github.chaogeoop.base.example.repository.es;

import io.github.chaogeoop.base.business.elasticsearch.BaseEs;
import io.github.chaogeoop.base.business.elasticsearch.EsField;
import io.github.chaogeoop.base.business.elasticsearch.EsTypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.List;

@Setter
@Getter
@JsonPropertyOrder({"familyId", "uid", "name", "tagIds", "address", "created"})
@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
//@EsTableName("estestines")
public class EsTestInEs extends BaseEs {
    @EsField(type = EsTypeEnum.LONG)
    private Long familyId;

    @EsField(type = EsTypeEnum.LONG)
    private Long uid;

    @EsField(type = EsTypeEnum.TEXT)
    private String name;

    @EsField(type = EsTypeEnum.LONG)
    private List<Long> tagIds;

    @EsField(type = EsTypeEnum.OBJECT, objectType = Address.class)
    private Address address;

    @EsField(type = EsTypeEnum.DATE)
    private Date created;

    @Setter
    @Getter
    @JsonPropertyOrder({"country", "province", "address", "addressList"})
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Address {
        @EsField(type = EsTypeEnum.TEXT, textHasKeywordField = true)
        private String country;

        @EsField(type = EsTypeEnum.KEYWORD)
        private String province;

        @EsField(type = EsTypeEnum.OBJECT, objectType = Address.class)
        private Address address;

        @EsField(type = EsTypeEnum.NESTED, objectType = Address.class)
        private List<Address> addressList;

        public static Address of(String country, String province, String listCountry, String listProvince) {
            Address data = new Address();
            Address subData = new Address();
            Address listData = new Address();

            subData.setCountry(country);
            subData.setProvince(province);

            listData.setCountry(listCountry);
            listData.setProvince(listProvince);

            data.setCountry(country);
            data.setProvince(province);
            data.setAddress(subData);
            data.setAddressList(Lists.newArrayList(listData));

            return data;
        }
    }
}
