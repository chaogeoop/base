package io.github.chaogeoop.base.business.redis;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public abstract class DistributedKeyType {
    private String type;

    private String subType;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistributedKeyType)) return false;
        DistributedKeyType that = (DistributedKeyType) o;
        return Objects.equal(type, that.type) && Objects.equal(subType, that.subType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type, subType);
    }
}
