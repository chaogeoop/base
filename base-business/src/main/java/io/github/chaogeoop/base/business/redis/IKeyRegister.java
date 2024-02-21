package io.github.chaogeoop.base.business.redis;

import java.util.List;

public interface IKeyRegister<T extends KeyType> {
    List<T> register();
}
