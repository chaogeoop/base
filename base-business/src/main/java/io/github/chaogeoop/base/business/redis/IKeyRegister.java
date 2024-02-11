package io.github.chaogeoop.base.business.redis;

import java.util.List;

public interface IKeyRegister<T extends DistributedKeyType> {
    List<T> register();
}
