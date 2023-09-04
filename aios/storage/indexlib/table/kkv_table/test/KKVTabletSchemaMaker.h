#pragma once

#include "autil/Log.h"
#include "indexlib/config/TabletSchema.h"

namespace indexlibv2::config {
class FieldConfig;
class KKVIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class AttributeConfig;
} // namespace indexlibv2::index

namespace indexlibv2::table {

class KKVTabletSchemaMaker
{
public:
    static std::shared_ptr<config::TabletSchema>
    Make(const std::string& fieldNames, const std::string& pkeyName, const std::string& skeyName,
         const std::string& valueNames, int64_t ttl = -1,
         const std::string& valueFormat = "" /*valueFormat = plain|impact*/, bool needStorePKValue = false);

private:
    static Status MakeFieldConfig(config::UnresolvedSchema* schema, const std::string& fieldNames);
    static Status MakeKKVIndexConfig(config::UnresolvedSchema* schema, const std::string& pkeyName,
                                     const std::string& skeyName, const std::string& valueNames, int64_t ttl = -1,
                                     const std::string& valueFormat = "");
    static void OptimizeKKVSKeyStore(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig,
                                     std::vector<std::shared_ptr<index::AttributeConfig>>& attrConfigs);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
