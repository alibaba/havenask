#pragma once

#include "autil/Log.h"
#include "indexlib/config/TabletSchema.h"

namespace indexlibv2::config {
class FieldConfig;
}

namespace indexlibv2::table {

class KVTabletSchemaMaker
{
public:
    // fieldNames format:
    //     fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    // keyName format:
    //     fieldName
    // ValueNames format:
    //     fieldName:updatable:compressStr
    static std::shared_ptr<config::TabletSchema>
    Make(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames, int64_t ttl = -1,
         const std::string& valueFormat = "" /*valueFormat = plain|impact|autoInline*/, bool needStorePKValue = false,
         bool useNumberHash = true);

private:
    static Status MakeFieldConfig(config::UnresolvedSchema* schema, const std::string& fieldNames);
    static Status MakeKVIndexConfig(config::UnresolvedSchema* schema, const std::string& keyName,
                                    const std::string& valueNames, int64_t ttl = -1,
                                    const std::string& valueFormat = "", bool useNumberHash = true);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
