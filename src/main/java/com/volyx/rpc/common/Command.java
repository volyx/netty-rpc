package com.volyx.rpc.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Command {
    private static final Logger log = LoggerFactory.getLogger(Command.class);
    private final String skeleton;
    private final String method;
    private final boolean staticMethod;
    private List<Object> parameters = new ArrayList<>();
    private List<Class> paramClasses = new ArrayList<>();

    public Command(String skeleton, String method, boolean staticMethod) {
        super();
        this.skeleton = skeleton;
        this.method = method;
        this.staticMethod = staticMethod;
    }

    public String getSkeleton() {
        return this.skeleton;
    }

    public String getMethod() {
        return this.method;
    }

    public boolean isStaticMethod() {
        return this.staticMethod;
    }

    public List<Object> getParameters() {
        return this.parameters;
    }

    public List<Class> getParamClasses() {
        return this.paramClasses;
    }

    public void addParameter(Object parameter, Class paramClass) {
        this.parameters.add(parameter);
        this.paramClasses.add(paramClass);
    }

    public Object[] findParameters() {
        Object[] parameters = new Object[0];
        if (this.parameters != null && false == this.parameters.isEmpty()) {
            parameters = this.parameters.toArray();
        }

        return parameters;
    }

    public Class[] findParamClasses() {
        Class[] paramClasses = new Class[0];
        if (this.paramClasses != null && false == this.paramClasses.isEmpty()) {
            paramClasses = new Class[this.paramClasses.size()];
            for (int i = 0; i < this.paramClasses.size(); i++) {
                paramClasses[i] = this.paramClasses.get(i);
            }
        }

        return paramClasses;
    }
}

