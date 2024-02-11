package io.github.chaogeoop.base.business.common.errors;

public class DistributedLockedException extends RuntimeException {
    public DistributedLockedException(String message) {
        super(message);
    }

}
