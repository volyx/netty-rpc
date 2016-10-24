package com.volyx.rpc.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Result {
    private static final Logger log = LoggerFactory.getLogger(Result.class);

    public static final Result VOID_RETURN = new Result(null, Void.class);

    public Result(Object returnObj, Class returnClass) {
        super();
        this.returnObj = returnObj;
        this.returnClass = returnClass;
        this.exceptional = Throwable.class.isAssignableFrom(returnClass);
    }

    private final Object returnObj;

    public Object getReturn() {
        return this.returnObj;
    }

    private final Class returnClass;

    public Class getReturnClass() {
        return this.returnClass;
    }

    private final boolean exceptional;

    public boolean isExceptional() {
        return this.exceptional;
    }
}

