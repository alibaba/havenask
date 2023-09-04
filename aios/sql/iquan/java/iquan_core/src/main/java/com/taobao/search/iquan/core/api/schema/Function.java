package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Function {
    private static final Logger logger = LoggerFactory.getLogger(Function.class);

    private static final String DETERMINISTIC = "deterministic";
    private static final String NON_DETERMINISTIC = "non-deterministic";

    private long version;
    private String name;
    private FunctionType type;
    private boolean isDeterministic;

    public Function(long version, String name, FunctionType type) {
        this(version, name, type, true);
    }

    public Function(long version, String name, FunctionType type, boolean isDeterministic) {
        this.version = version;
        this.name = name;
        this.type = type;
        this.isDeterministic = isDeterministic;
    }

    public final long getVersion() {
        return version;
    }

    public final String getName() {
        return name;
    }

    public final FunctionType getType() {
        return type;
    }

    public final boolean isDeterministic() {
        return isDeterministic;
    }

    public abstract boolean isValid();

    protected final boolean isBaseValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(version <= 0, "function version is not a positive number");
            ExceptionUtils.throwIfTrue(name.isEmpty(), "name is empty");
            ExceptionUtils.throwIfTrue(type == FunctionType.FT_INVALID,
                                        "type is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public String getDigest() {
        return String.format("function_%s_%s_%s_%d", isDeterministic ? DETERMINISTIC : NON_DETERMINISTIC,
                type.getName(), name, version);
    }
}
