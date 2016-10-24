package com.volyx.rpc.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Response {
    private static final Logger log = LoggerFactory.getLogger(Response.class);

    public Response(Long id, Result result, String resultClass) {
        super();
        this.id = id;
        this.result = result;
        this.resultClass = resultClass;
    }

    private final Long id;
    private final Result result;
    private final String resultClass;

    public Long getId() {
        return this.id;
    }

    public Result getResult() {
        return this.result;
    }

    public String getResultClass() {
        return this.resultClass;
    }

    @Override
    public String toString() {
        String format = String.format("Response-%d", this.id);

        return format;
    }
}
