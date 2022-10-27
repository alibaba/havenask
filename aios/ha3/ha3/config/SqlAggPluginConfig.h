#pragma once

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <build_service/plugin/ModuleInfo.h>

BEGIN_HA3_NAMESPACE(config);

class SqlAggPluginConfig : public autil::legacy::Jsonizable {
public:
    SqlAggPluginConfig() {}
    ~SqlAggPluginConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("modules", modules, modules);
    }
public:
    build_service::plugin::ModuleInfos modules;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlAggPluginConfig);

END_HA3_NAMESPACE(config);

