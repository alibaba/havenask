#include <autil/StringUtil.h>
#include "indexlib/config/primary_key_load_strategy_param.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PrimaryKeyLoadStrategyParam);

const std::string PrimaryKeyLoadStrategyParam::COMBINE_SEGMENTS = "combine_segments";
const std::string PrimaryKeyLoadStrategyParam::MAX_DOC_COUNT = "max_doc_count";
const std::string PrimaryKeyLoadStrategyParam::LOOKUP_REVERSE = "lookup_reverse";
const std::string PrimaryKeyLoadStrategyParam::PARAM_SEP = ",";
const std::string PrimaryKeyLoadStrategyParam::KEY_VALUE_SEP = "=";
void PrimaryKeyLoadStrategyParam::FromString(const string& paramStr)
{
    if (paramStr.empty())
    {
        return;
    }

    vector<vector<string> > params;
    StringUtil::fromString(paramStr, params, "=", ",");
    if (!ParseParams(params))
    {
        INDEXLIB_FATAL_ERROR(
                BadParameter,
                "primary key load param [%s] not legal", paramStr.c_str());
    }
}

void PrimaryKeyLoadStrategyParam::CheckParam(const std::string& param)
{
    PrimaryKeyLoadStrategyParam loadParam;
    loadParam.FromString(param);
}

bool PrimaryKeyLoadStrategyParam::ParseParams(const vector<vector<string> >& params)
{
    for (size_t i = 0; i < params.size(); i++)
    {
        const vector<string>& param = params[i];
        if (param.size() != 2)
        {
            return false;
        }
        if (!ParseParam(param[0], param[1]))
        {
            return false;
        }
    }
    return true;
}

bool PrimaryKeyLoadStrategyParam::ParseParam(const string& name, const string& value)
{
    if (name == COMBINE_SEGMENTS)
    {
        return StringUtil::fromString(value, mNeedCombineSegments);
    }
    if (name == MAX_DOC_COUNT)
    {
        return StringUtil::fromString(value, mMaxDocCount);
    }
    if (name == LOOKUP_REVERSE)
    {
        if (mNeedLookupReverse)
        {
            return true;
        }
        return StringUtil::fromString(value, mNeedLookupReverse);
    }
    return false;
}

void PrimaryKeyLoadStrategyParam::AssertEqual(
        const PrimaryKeyLoadStrategyParam& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mLoadMode, other.mLoadMode, 
                           "pk load mode not equal");
    IE_CONFIG_ASSERT_EQUAL(mMaxDocCount, other.mMaxDocCount, 
                           "max doc count not equal");
    IE_CONFIG_ASSERT_EQUAL(mNeedCombineSegments, other.mNeedCombineSegments, 
                           "need combine segments not equal");
}

IE_NAMESPACE_END(config);

