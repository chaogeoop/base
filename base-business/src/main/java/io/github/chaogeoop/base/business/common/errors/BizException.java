package io.github.chaogeoop.base.business.common.errors;

public class BizException extends RuntimeException {
    public BizException(String msg) {
        super(msg);
    }

    public BizException(String msg, Exception error) {
        super(msg);

        this.initCause(error);
    }

    public BizException(Exception error) {
        super(error.getMessage());

        this.initCause(error);
    }
}
