package io.github.chaogeoop.base.business.elasticsearch;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Collection;

public class IBaseEsClassSerializer<M extends IBaseEs> extends JsonSerializer<M> {
    private final EsHelper.FieldNode tree;

    IBaseEsClassSerializer(EsHelper.FieldNode tree) {
        this.tree = tree;
    }

    @Override
    public void serialize(M data, JsonGenerator gen, SerializerProvider provider) throws IOException {
        pickEsField(this.tree, data, gen, provider);
    }

    private void pickEsField(EsHelper.FieldNode node, Object data, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();

        for (EsHelper.FieldNode child : node.getChildren()) {
            try {
                EsHelper.FieldDetail detail = child.getDetail();
                Object fieldValue = detail.getField().get(data);

                if (fieldValue == null) {
                    gen.writeNullField(detail.getField().getName());
                    continue;
                }

                if (EsTypeEnum.OBJECT.equals(detail.getEsField().type())) {
                    gen.writeFieldName(detail.getField().getName());
                    this.pickEsField(child, fieldValue, gen, provider);
                    continue;
                }

                if (EsTypeEnum.NESTED.equals(detail.getEsField().type())) {
                    gen.writeFieldName(detail.getField().getName());
                    gen.writeStartArray();
                    for (Object o : (Collection) fieldValue) {
                        if (o == null) {
                            gen.writeNull();
                            continue;
                        }
                        this.pickEsField(child, o, gen, provider);
                    }
                    gen.writeEndArray();
                    continue;
                }

                provider.defaultSerializeField(detail.getField().getName(), fieldValue, gen);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        gen.writeEndObject();
    }
}
