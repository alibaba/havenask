#ifndef __INDEXLIB_TRUNCATE_CONFIG_MAKER_H
#define __INDEXLIB_TRUNCATE_CONFIG_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_option_config.h"

IE_NAMESPACE_BEGIN(config);

struct TruncateParams
{
    std::string truncCommonStr;
    std::string distinctStr;
    std::string truncIndexConfigStr;
    std::string strategyType;

    TruncateParams() {}
    TruncateParams(const std::string& str1, 
                   const std::string& str2, 
                   const std::string& str3,
                   const std::string& str4 = "default")
        : truncCommonStr(str1)
        , distinctStr(str2)
        , truncIndexConfigStr(str3)
        , strategyType(str4)
    {}
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
    static TruncateOptionConfigPtr MakeConfig(
            const std::string& truncCommonStr,
            const std::string& distinctStr,
            const std::string& truncIndexConfigStr,
            const std::string& strategyType = "default");


    static void RewriteSchema(
        const IndexPartitionSchemaPtr& schema, const TruncateParams& params);

    static TruncateOptionConfigPtr MakeConfig(const TruncateParams& params);
    static TruncateOptionConfigPtr MakeConfigFromJson(const std::string& jsonStr);

private:
    static void ParseCommon(const std::string& str, 
                            TruncateStrategyPtr &strategy);

    static void ParseDistinct(const std::string& str, 
                              DiversityConstrain &constrain);

    static void ParseIndexConfigs(
            const std::string& str, 
            TruncateOptionConfigPtr &truncOptionConfig, 
            TruncateStrategyPtr &truncateStrategy, 
            DiversityConstrain &diverConstrain,
            const std::string &strategyType);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateConfigMaker);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TRUNCATE_CONFIG_MAKER_H
