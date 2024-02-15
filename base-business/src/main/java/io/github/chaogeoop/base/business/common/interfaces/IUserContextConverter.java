package io.github.chaogeoop.base.business.common.interfaces;

import org.springframework.web.context.request.NativeWebRequest;

public interface IUserContextConverter {
    IUserContext convert(NativeWebRequest request);
}
