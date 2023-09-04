#ifndef __INDEXLIB_TRUNCATE_CONFIG_MAKER_H
#define __INDEXLIB_TRUNCATE_CONFIG_MAKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_option_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

struct TruncateParams {
    std::string truncCommonStr;
    std::string distinctStr;
    std::string truncIndexConfigStr;
    std::string strategyType;
    std::string truncateProfile2PayloadNames;

    int64_t reTruncateLimit;
    std::string reTruncateDistincStr;
    std::string reTruncateFilterStr;

    TruncateParams() {}
    TruncateParams(const std::string& str1, const std::string& str2, const std::string& str3,
                   const std::string& inputStrategyType)
        : truncCommonStr(str1)
        , distinctStr(str2)
        , truncIndexConfigStr(str3)
        , strategyType(inputStrategyType)
        , reTruncateLimit(-1)
    {
    }
    TruncateParams(const std::string& str1, const std::string& str2, const std::string& str3,
                   const std::string& inputStrategyType, const std::string& inputTruncateProfile2PayloadNames)
        : truncCommonStr(str1)
        , distinctStr(str2)
        , truncIndexConfigStr(str3)
        , strategyType(inputStrategyType)
        , truncateProfile2PayloadNames(inputTruncateProfile2PayloadNames)
        , reTruncateLimit(-1)
    {
    }
};

class TruncateConfigMaker
{
public:
    TruncateConfigMaker();
    ~TruncateConfigMaker();

public:
    // truncCommonStr format:
    //        truncThreshold:truncLimit
    // distinctStr format:
    //        distField:distCount:distExpandLimit
    // truncIndexConfigStr format:
    //        indexName=profileName:sortField#sortType|sortField#sortType,...;indexName=...
    static TruncateOptionConfigPtr MakeConfig(const std::string& truncCommonStr, const std::string& distinctStr,
                                              const std::string& truncIndexConfigStr,
                                              const std::string& strategyType = "default", int64_t reTruncateLimit = -1,
                                              const std::string& reTruncateDistincStr = "",
                                              const std::string& reTruncateFilterStr = "");

    static void RewriteSchema(const IndexPartitionSchemaPtr& schema, const TruncateParams& params);

    static TruncateOptionConfigPtr MakeConfig(const TruncateParams& params);
    static TruncateOptionConfigPtr MakeConfigFromJson(const std::string& jsonStr);

private:
    static void ParseCommon(const std::string& str, TruncateStrategyPtr& strategy);

    static void ParseDistinct(const std::string& str, DiversityConstrain& constrain);

    static void ParseIndexConfigs(const std::string& str, const std::string& strategyType,
                                  TruncateOptionConfigPtr& truncOptionConfig, TruncateStrategyPtr& truncateStrategy,
                                  DiversityConstrain& diverConstrain);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateConfigMaker);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_CONFIG_MAKER_H
