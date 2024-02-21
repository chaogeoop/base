package io.github.chaogeoop.base.business.redis;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Setter
@Getter
public class KeyEntity<T extends KeyType> {
    private T type;

    private String typeId;

    public static <M extends KeyType> KeyEntity<M> of(M type, String typeId) {
        KeyEntity<M> data = new KeyEntity<>();

        data.setType(type);
        data.setTypeId(typeId);

        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyEntity<?> keyEntity = (KeyEntity<?>) o;
        return Objects.equals(type, keyEntity.type) && Objects.equals(typeId, keyEntity.typeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, typeId);
    }
}
