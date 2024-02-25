package io.github.chaogeoop.base.business.common.helpers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;

public class JsonHelper {
    private static final ObjectMapper OM = new ObjectMapper();

    static {
        OM.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        OM.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        OM.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }

    public static <T> T readValue(String json, Class<T> clazz) {
        if (String.class == clazz) {
            return (T) json;
        }

        if (StringUtils.isBlank(json)) {
            return null;
        }

        try {
            return OM.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <T> T readValue(String json, TypeReference<T> type) {
        if (StringUtils.isBlank(json)) {
            return null;
        }

        try {
            return OM.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <T> String writeValueAsString(T value) {
        try {
            if (value instanceof String) {
                return (String) value;
            }

            return OM.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <T> T convert(Object value, Class<T> clazz) {
        try {
            if (value == null) {
                return null;
            }

            if (value instanceof String) {
                return readValue((String) value, clazz);
            }

            return OM.convertValue(value, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static <T> T convert(Object value, TypeReference<T> type) {
        try {
            if (value == null) {
                return null;
            }

            if (value instanceof String) {
                return readValue((String) value, type);
            }

            return OM.convertValue(value, type);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
