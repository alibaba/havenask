package com.taobao.search.iquan.core.catalog.function;

import com.taobao.search.iquan.core.api.schema.UdxfFunction;
import com.taobao.search.iquan.core.api.schema.UdxfSignature;
import com.taobao.search.iquan.core.catalog.function.internal.TableFunction;
import com.taobao.search.iquan.core.utils.IquanTypeFactory;
import org.apache.calcite.sql.SqlFunction;

import java.util.List;
import java.util.stream.Collectors;

public class IquanTableFunction extends IquanFunction {
    public IquanTableFunction(IquanTypeFactory typeFactory, UdxfFunction function) {
        super(typeFactory, function);
    }

    @Override
    public SqlFunction build() {
        return new TableFunction(
                typeFactory,
                (UdxfFunction) function
        );
    }

    @Override
    protected List<String> getSignatures() {
        List<UdxfSignature> signatureList = ((UdxfFunction) function).getSignatureList();
        return signatureList.stream().map(v -> v.getDigest()).collect(Collectors.toList());
    }
}
