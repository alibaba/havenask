#pragma once

#include "autil/Log.h"
#include "indexlib/config/FieldConfig.h"

namespace indexlibv2::config {

class FieldConfigMaker
{
public:
    // fieldNamesStr format:
    //     fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    static std::vector<std::shared_ptr<config::FieldConfig>> Make(const std::string& fieldNamesStr);

    static std::vector<std::string> SplitToStringVector(const std::string& names);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
