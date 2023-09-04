package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.client.common.json.function.JsonTvfFunction;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class TvfFunction extends Function {
    private static final Logger logger = LoggerFactory.getLogger(TvfFunction.class);

    private final List<TvfSignature> signatureList;
    private final JsonTvfFunction jsonTvfFunction;

    public TvfFunction(long version, String name, FunctionType type, boolean isDeterministic, List<TvfSignature> signatureList) {
        this(version, name, type, isDeterministic, signatureList, null);
    }

    public TvfFunction(long version, String name, FunctionType type, boolean isDeterministic,
                       List<TvfSignature> signatureList, JsonTvfFunction jsonTvfFunction) {
        super(version, name, type, isDeterministic);
        this.signatureList = signatureList;
        this.jsonTvfFunction = jsonTvfFunction;
    }

    public List<TvfSignature> getSignatureList() {
        return signatureList;
    }

    public JsonTvfFunction getJsonTvfFunction() {
        return jsonTvfFunction;
    }

    @Override
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(!isBaseValid(), "base valid function return false");
            ExceptionUtils.throwIfTrue(getType() != FunctionType.FT_TVF, "type should be TVF");
            ExceptionUtils.throwIfTrue(signatureList.isEmpty(),
                    "function signature is empty");
            ExceptionUtils.throwIfTrue(signatureList.stream().anyMatch(v -> !v.isValid()),
                    "one of signature is not valid");
            ExceptionUtils.throwIfTrue(jsonTvfFunction == null, "json tvf function is null");
            ExceptionUtils.throwIfTrue(!jsonTvfFunction.isValid(), "json tvf function is not valid");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        return isValid;
    }

    @Override
    public String getDigest() {
        StringBuilder sb = new StringBuilder(256);
        sb.append(super.getDigest()).append("|");
        List<String> content = signatureList.stream().map(v -> v.getDigest()).collect(Collectors.toList());
        sb.append(String.join("|", content));
        return sb.toString();
    }

    @Override
    public String toString() {
        return getDigest();
    }
}
