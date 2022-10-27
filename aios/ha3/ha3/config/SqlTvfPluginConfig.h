#pragma once

#include <ha3/config/SqlTvfProfileInfo.h>
#include <build_service/plugin/ModuleInfo.h>

BEGIN_HA3_NAMESPACE(config);

class SqlTvfPluginConfig : public autil::legacy::Jsonizable {
public:
    SqlTvfPluginConfig() {}
    ~SqlTvfPluginConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("modules", modules, modules);
        json.Jsonize("tvf_profiles", tvfProfileInfos, tvfProfileInfos);
    }
public:
    build_service::plugin::ModuleInfos modules;
    SqlTvfProfileInfos tvfProfileInfos;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlTvfPluginConfig);

END_HA3_NAMESPACE(config);
