package io.learning.flink.utils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author taox
 * Created by taox on 2019/6/25.
 */
@Slf4j
public class JsonUtils {

	public static final ObjectMapper objectMapper = new ObjectMapper();

	static {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
	}

	public static String toJson(Object object) {
		try {
			return objectMapper.writeValueAsString(object);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public static <T> T jsonToObject(String json, Class<T> clazz) {
		try {
			return objectMapper.readValue(json, clazz);
		} catch (IOException e) {
			log.error(String.format("parse json %s failed.", json), e);
			return null;
		}
	}

	public static <T> T jsonToObject(String json, TypeReference typeReference) {
		try {
			if (json == null) {
				return null;
			}
			return (T) objectMapper.readValue(json, typeReference);
		} catch (IOException e) {
			log.error("parse json failed.", e);
			return null;
		}
	}

}

