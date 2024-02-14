package io.github.chaogeoop.base.business.common.interfaces;

import io.github.chaogeoop.base.business.common.entities.BaseUserContext;
import org.springframework.web.context.request.NativeWebRequest;

public interface IUserContextConverter {
    BaseUserContext convert(NativeWebRequest request);
}
