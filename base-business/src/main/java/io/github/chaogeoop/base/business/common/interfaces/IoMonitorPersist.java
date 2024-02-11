package io.github.chaogeoop.base.business.common.interfaces;

import io.github.chaogeoop.base.business.common.entities.IoStatistic;

public interface IoMonitorPersist {
    void handle(IoStatistic ioStatistic);
}
