package com.volyx.rpc.common.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.volyx.rpc.common.JsonSerializer;
import com.volyx.rpc.common.Request;
import com.volyx.rpc.common.Response;

import java.util.Date;

public class GsonSerializer extends JsonSerializer {
    private static final Gson DEFAULT_GSON;

    static {
        // create one Gson Builder and reuse it.
        GsonBuilder GSON_BUILDER = new GsonBuilder();
        GSON_BUILDER.enableComplexMapKeySerialization();

        // register the three kinds of Type Adapters to handle requests, responses, and date objects
        GSON_BUILDER.registerTypeAdapter(Request.class, new RequestTypeAdapter());
        GSON_BUILDER.registerTypeAdapter(Response.class, new ResponseTypeAdapter());
//        GSON_BUILDER.registerTypeAdapter(Date.class, new DateTypeAdapter());
        GSON_BUILDER.serializeNulls();

        DEFAULT_GSON = GSON_BUILDER.create();
    }

    public GsonSerializer() {
        super();
    }

    public String toJson(Object src, Class srcClass) {
        return DEFAULT_GSON.toJson(src, srcClass);
    }

    public JsonElement toJsonElement(Object src) {
        return DEFAULT_GSON.toJsonTree(src);
    }

    public <T> T fromJson(String jsonString, Class<T> srcClass) {
        return DEFAULT_GSON.fromJson(jsonString, srcClass);
    }

    public <T> T fromJsonElement(JsonElement jsonElement, Class<T> srcClass) {
        return DEFAULT_GSON.fromJson(jsonElement, srcClass);
    }
}

