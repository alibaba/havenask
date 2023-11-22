package com.taobao.search.iquan.core.api.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UdxfSignature {
    protected static final Logger logger = LoggerFactory.getLogger(UdxfSignature.class);

    protected String digest;
    protected boolean variableArgs = false;
    protected List<AbstractField> paramTypes = new ArrayList<>();
    protected List<AbstractField> returnTypes = new ArrayList<>();
    protected List<AbstractField> accTypes = new ArrayList<>();

    // for internal use
    protected List<RelDataType> paramRelTypes = null;
    protected RelDataType returnRelType = null;
    protected List<RelDataType> accRelTypes = null;

    protected UdxfSignature() {
    }

    public static Builder newBuilder(FunctionType type) {
        return new Builder(type);
    }

    @Override
    public String toString() {
        return getDigest();
    }

    public String getDigest() {
        return digest;
    }

    private void setDigest(String digest) {
        this.digest = digest;
    }

    public boolean isVariableArgs() {
        return variableArgs;
    }

    public void setVariableArgs(boolean variableArgs) {
        this.variableArgs = variableArgs;
    }

    public List<AbstractField> getParamTypes() {
        return paramTypes;
    }

    private void setParamTypes(List<AbstractField> types) {
        this.paramTypes.clear();
        this.paramTypes.addAll(types);
    }

    private void addParamType(AbstractField type) {
        this.paramTypes.add(type);
    }

    public List<AbstractField> getReturnTypes() {
        return returnTypes;
    }

    private void setReturnTypes(List<AbstractField> types) {
        this.returnTypes.clear();
        this.returnTypes.addAll(types);
    }

    private void addReturnType(AbstractField type) {
        this.returnTypes.add(type);
    }

    public List<AbstractField> getAccTypes() {
        return accTypes;
    }

    private void setAccTypes(List<AbstractField> types) {
        this.accTypes.clear();
        this.accTypes.addAll(types);
    }

    private void addAccType(AbstractField type) {
        this.accTypes.add(type);
    }

    public List<RelDataType> getParamRelTypes() {
        return paramRelTypes;
    }

    public RelDataType getReturnRelType() {
        return returnRelType;
    }

    public List<RelDataType> getAccRelTypes() {
        return accRelTypes;
    }

    protected abstract boolean doValid();

    public boolean isValid() {
        if (!doValid()) {
            return false;
        }

        try {
            ExceptionUtils.throwIfTrue(paramTypes.stream().anyMatch(v -> !v.isValid()), "one of inputs is not valid");
            ExceptionUtils.throwIfTrue(returnTypes.stream().anyMatch(v -> !v.isValid()), "one of returns is not valid");
            ExceptionUtils.throwIfTrue(accTypes.stream().anyMatch(v -> !v.isValid()), "one of acc types is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            return false;
        }

        paramRelTypes = IquanTypeFactory.DEFAULT.createRelTypeList(paramTypes);
        returnRelType = IquanTypeFactory.DEFAULT.createStructRelType(returnTypes);
        accRelTypes = IquanTypeFactory.DEFAULT.createRelTypeList(accTypes);

        if (paramRelTypes == null
                || returnRelType == null
                || accRelTypes == null) {
            logger.error("udxf signature valid fail: convert AbstractField to RelDataType fail");
            return false;
        }
        return true;
    }

    private void calcDigest(String name) {
        StringBuilder sb = new StringBuilder(256);
        List<String> returnContent = returnTypes.stream().map(AbstractField::getDigest).collect(Collectors.toList());
        sb.append("(").append(String.join(",", returnContent)).append(")");

        sb.append("UDXF_").append(name);

        List<String> paramContent = paramTypes.stream().map(AbstractField::getDigest).collect(Collectors.toList());
        sb.append("(").append(String.join(",", paramContent));
        if (variableArgs) {
            sb.append("...");
        }
        sb.append(")");

        if (accTypes.size() > 0) {
            List<String> accContent = accTypes.stream().map(AbstractField::getDigest).collect(Collectors.toList());
            sb.append("[").append(String.join(",", accContent)).append("]");
        }

        setDigest(sb.toString());
    }

    public static class Builder {
        private UdxfSignature signature;

        public Builder(FunctionType type) {
            switch (type) {
                case FT_UDF:
                    this.signature = new UdfSignature();
                    break;
                case FT_UDAF:
                    this.signature = new UdafSignature();
                    break;
                case FT_UDTF:
                    this.signature = new UdtfSignature();
                    break;
                default:
                    throw new UnsupportedOperationException("UdxfSignature.Builder: not support function type " + type.getName());
            }
        }

        public Builder variableArgs(boolean variableArgs) {
            signature.setVariableArgs(variableArgs);
            return this;
        }

        public Builder paramTypes(List<AbstractField> params) {
            signature.setParamTypes(params);
            return this;
        }

        public Builder addParamType(AbstractField type) {
            signature.addParamType(type);
            return this;
        }

        public Builder returnTypes(List<AbstractField> returns) {
            signature.setReturnTypes(returns);
            return this;
        }

        public Builder addReturnType(AbstractField type) {
            signature.addReturnType(type);
            return this;
        }

        public Builder accTypes(List<AbstractField> accTypes) {
            signature.setAccTypes(accTypes);
            return this;
        }

        public Builder addAccTypes(AbstractField accType) {
            signature.addAccType(accType);
            return this;
        }

        public UdxfSignature build(String name) {
            if (signature.getDigest() == null || signature.getDigest().isEmpty()) {
                signature.calcDigest(name);
            }
            if (signature.isValid()) {
                return signature;
            }
            logger.error("UdxfSignature.Builder: build fail for {}", signature.getDigest());
            return null;
        }
    }
}
