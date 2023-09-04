package com.taobao.search.iquan.core.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

public class IquanCalciteTypeFactory extends SqlTypeFactoryImpl {
    public IquanCalciteTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }
}
