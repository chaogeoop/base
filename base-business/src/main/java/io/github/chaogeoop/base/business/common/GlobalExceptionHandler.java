package io.github.chaogeoop.base.business.common;

import io.github.chaogeoop.base.business.common.errors.BizException;
import io.github.chaogeoop.base.business.common.entities.HttpResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Slf4j
public class GlobalExceptionHandler {
    @ExceptionHandler(Exception.class)
    public HttpResult<Object> errorHandler(HttpServletRequest request, HttpServletResponse response, Exception e) {
        if (e instanceof BizException) {
            return HttpResult.error(HttpResult.ErrorCodeEnum.BIZ_ERROR, e.getMessage());
        } else {
            log.error("internal error : {} {}", e.getMessage(), e);
            return HttpResult.error(HttpResult.ErrorCodeEnum.INTERNAL_ERROR, String.format("internal error: %s", e.getMessage()));
        }
    }
}
