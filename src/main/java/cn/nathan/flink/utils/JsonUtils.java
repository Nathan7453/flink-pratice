package cn.nathan.flink.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtils {
    private static final Logger log = LoggerFactory.getLogger(JsonUtils.class);

    private static ObjectMapper deserializeMapper;

    private static ObjectMapper serializeMapper;

    static {
        init();
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        ObjectMapper mapper = getDeserializeMapper();
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            log.error("error = {}, json = {}", e.getMessage(), json, e);
            throw new RuntimeException(" result can not converto to Object");
        }

    }

    public static <T> T fromJson(String json, TypeReference<T> typereference) {
        ObjectMapper mapper = getDeserializeMapper();
        try {
            return mapper.readValue(json, typereference);
        } catch (IOException e) {
            log.error("error = {}, json = {}", e.getMessage(), json, e);
            throw new RuntimeException(" result can not converto to Object");
        }
    }


    public static JsonNode fromJson(String json) {
        ObjectMapper mapper = getDeserializeMapper();
        JsonFactory factory = mapper.getFactory();
        try {
            JsonParser jp = factory.createParser(json);
            JsonNode jsonNode = mapper.readTree(jp);
            return jsonNode;
        } catch (IOException e) {
            log.error("error = {}, json = {}", e.getMessage(), json, e);
            throw new RuntimeException(" result can not converto to Object");
        }

    }

    public static <T> T fromJson(JsonNode json, Class<T> clazz) {
        ObjectMapper mapper = getDeserializeMapper();
        try {
            return mapper.treeToValue(json, clazz);
        } catch (IOException e) {
            log.error("error = {}, json = {}", e.getMessage(), json, e);
            throw new RuntimeException(" result can not converto to Object");
        }
    }


    public static <T> T fromJson(JsonNode json, TypeReference<T> typeReference) {
        ObjectMapper mapper = getDeserializeMapper();
        try {
            return mapper.convertValue(json, typeReference);
        } catch (Exception e) {
            log.error("error = {}, json = {}", e.getMessage(), json, e);
            throw new RuntimeException(" result can not converto to Object");
        }
    }


    private static ObjectMapper getSerializeMapper() {
        return serializeMapper;
    }

    private static ObjectMapper getDeserializeMapper() {
        return deserializeMapper;
    }

    private static void init() {
        serializeMapper = new ObjectMapper();
        serializeMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        serializeMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        deserializeMapper = new ObjectMapper();
        deserializeMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        deserializeMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    }

    public static String toJsonString(Object obj) {
        try {
            return getSerializeMapper().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
        return "";
    }

}
