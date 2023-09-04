package com.taobao.search.iquan.core.utils;

public class IquanClassLoader extends ClassLoader {

    static {
        ClassLoader.registerAsParallelCapable();
    }

    public IquanClassLoader(ClassLoader parent) {
        super(parent);
    }

    public IquanClassLoader() {
    }

    @Override
    public final Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("ExpressionReducer$")) {
            return null;
        } else {
            return super.loadClass(name, resolve);
        }
    }
}
