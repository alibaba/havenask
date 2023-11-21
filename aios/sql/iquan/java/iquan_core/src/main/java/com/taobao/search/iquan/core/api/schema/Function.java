package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.catalog.FullPath;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public abstract class Function {
    private static final Logger logger = LoggerFactory.getLogger(Function.class);

    private static final String DETERMINISTIC = "deterministic";
    private static final String NON_DETERMINISTIC = "non-deterministic";

    private final long version;
    private final String name;
    private final FunctionType type;
    private final boolean isDeterministic;
    private final FullPath fullPath;


    public Function(long version, FullPath fullPath, FunctionType type) {
        this(version, fullPath, type, true);
    }

    public Function(long version, FullPath fullPath, FunctionType type, boolean isDeterministic) {
        this.version = version;
        this.fullPath = fullPath;
        this.name = fullPath.getObjectName();
        this.type = type;
        this.isDeterministic = isDeterministic;
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
