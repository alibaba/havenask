#include <autil/StringUtil.h>
#include "indexlib/config/merge_strategy_parameter.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, MergeStrategyParameter);

MergeStrategyParameter::MergeStrategyParameter() 
{
}

MergeStrategyParameter::~MergeStrategyParameter() 
{
}

void MergeStrategyParameter::Jsonize(
        autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("input_limits", inputLimitParam, inputLimitParam);
    json.Jsonize("strategy_conditions", strategyConditions, strategyConditions);
    json.Jsonize("output_limits", outputLimitParam, outputLimitParam);
}

void MergeStrategyParameter::SetLegacyString(const string& legacyStr)
{
    inputLimitParam = "";
    strategyConditions = legacyStr;
    outputLimitParam = "";
}

string MergeStrategyParameter::GetLegacyString() const
{
    vector<string> strVec;
    if (!inputLimitParam.empty())
    {
        strVec.push_back(inputLimitParam);
    }
    
    if (!strategyConditions.empty())
    {
        strVec.push_back(strategyConditions);
    }

    if (!outputLimitParam.empty())
    {
        strVec.push_back(outputLimitParam);
    }

    if (strVec.empty())
    {
        return string("");
    }
    return StringUtil::toString(strVec, ";");
}

IE_NAMESPACE_END(config);

