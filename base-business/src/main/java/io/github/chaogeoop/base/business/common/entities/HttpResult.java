package io.github.chaogeoop.base.business.common.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HttpResult<T> {
    private int code = 200;

    private String message;

    private T data;

    private HttpResult() {

    }

    public static <T> HttpResult<T> of(T data) {
        HttpResult<T> result = new HttpResult<>();

        result.data = data;

        return result;
    }

    public static <T> HttpResult<T> error(ErrorCodeEnum errorCode, String message) {
        HttpResult<T> result = new HttpResult<>();

        result.code = errorCode.getCode();
        result.message = message;

        return result;
    }


    public enum ErrorCodeEnum {
        BIZ_ERROR(100),
        INTERNAL_ERROR(101),
        ;

        private final int code;

        public int getCode() {
            return this.code;
        }

        ErrorCodeEnum(int code) {
            this.code = code;
        }
    }
}
