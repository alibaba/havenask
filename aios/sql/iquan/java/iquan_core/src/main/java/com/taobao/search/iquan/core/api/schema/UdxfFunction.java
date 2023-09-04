package com.taobao.search.iquan.core.api.schema;

import com.taobao.search.iquan.client.common.json.function.JsonUdxfFunction;
import com.taobao.search.iquan.core.api.exception.ExceptionUtils;
import com.taobao.search.iquan.core.api.exception.IquanNotValidateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UdxfFunction extends Function {
    private static final Logger logger = LoggerFactory.getLogger(UdxfFunction.class);

    private final List<UdxfSignature> signatureList;
    private final JsonUdxfFunction jsonUdxfFunction;

    public UdxfFunction(long version, String name, FunctionType type, List<UdxfSignature> signatureList) {
        this(version, name, type, signatureList, new JsonUdxfFunction());
    }

    public UdxfFunction(long version, String name, FunctionType type, List<UdxfSignature> signatureList, JsonUdxfFunction jsonUdxfFunction) {
        super(version, name, type);
        this.signatureList = signatureList;
        this.jsonUdxfFunction = jsonUdxfFunction;
    }

    public UdxfFunction(long version, String name, FunctionType type, boolean isDeterministic, List<UdxfSignature> signatureList) {
        this(version, name, type, isDeterministic, signatureList, new JsonUdxfFunction());
    }

    public UdxfFunction(long version, String name, FunctionType type, boolean isDeterministic,
                        List<UdxfSignature> signatureList, JsonUdxfFunction jsonUdxfFunction) {
        super(version, name, type, isDeterministic);
        this.signatureList = signatureList;
        this.jsonUdxfFunction = jsonUdxfFunction;
    }

    public List<UdxfSignature> getSignatureList() {
        return signatureList;
    }

    public JsonUdxfFunction getJsonUdxfFunction() {
        return jsonUdxfFunction;
    }

    @Override
    public boolean isValid() {
        boolean isValid = true;
        try {
            ExceptionUtils.throwIfTrue(!isBaseValid(), "base valid function return false");
            ExceptionUtils.throwIfTrue(getType() != FunctionType.FT_UDF
                    && getType() != FunctionType.FT_UDAF
                    && getType() != FunctionType.FT_UDTF, "type should be UDF or UDAF or UDTF");
            ExceptionUtils.throwIfTrue(signatureList.isEmpty(),
                    "function signature is empty");
            ExceptionUtils.throwIfTrue(signatureList.stream().anyMatch(v -> !v.isValid()),
                    "one of signature is not valid");
            ExceptionUtils.throwIfTrue(jsonUdxfFunction == null, "json udxf function is null");
        } catch (IquanNotValidateException e) {
            logger.error(e.getMessage());
            isValid = false;
        }
        if (!isValid) {
            return false;
        }

        Set<String> dupDigestSet = new HashSet<>(signatureList.size() * 2);
        for (UdxfSignature signature : signatureList) {
            String digest = signature.getDigest();
            if (dupDigestSet.contains(digest)) {
                logger.error("Udxf function: duplicated signatures found in function {}, {}", getName(), digest);
                return false;
            }
            dupDigestSet.add(digest);
        }
        return true;
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
