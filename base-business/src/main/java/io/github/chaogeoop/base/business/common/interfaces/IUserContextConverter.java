package io.github.chaogeoop.base.business.common.interfaces;

import io.github.chaogeoop.base.business.common.entities.UserContext;
import org.springframework.web.context.request.NativeWebRequest;

public interface IUserContextConverter {
    UserContext convert(NativeWebRequest request);
}
