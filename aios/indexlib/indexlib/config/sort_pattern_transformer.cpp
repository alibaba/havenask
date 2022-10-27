#include "indexlib/config/sort_pattern_transformer.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SortPatternTransformer);

SortPatternTransformer::SortPatternTransformer() 
{
}

SortPatternTransformer::~SortPatternTransformer() 
{
}

void SortPatternTransformer::FromStringVec(
        const StringVec& sortPatternStrVec,
        SortPatternVec& sortPatterns)
{
    for (size_t i = 0; i < sortPatternStrVec.size(); ++i)
    {
        sortPatterns.push_back(
                FromString(sortPatternStrVec[i]));
    }
}

void SortPatternTransformer::ToStringVec(
        const SortPatternVec& sortPatterns, 
        StringVec& sortPatternStrVec)
{
    for (size_t i = 0; i < sortPatterns.size(); ++i)
    {
        sortPatternStrVec.push_back(
                ToString(sortPatterns[i]));
    }
}

SortPattern SortPatternTransformer::FromString(
        const string& sortPatternStr)
{
    string upperCaseStr = sortPatternStr;
    StringUtil::toUpperCase(upperCaseStr);
    if (upperCaseStr == ASC_SORT_PATTERN)
    {
        return sp_asc;
    }
    return sp_desc;
}

std::string SortPatternTransformer::ToString(
        const SortPattern& sortPattern)
{
    if (sortPattern == sp_desc)
    {
        return DESC_SORT_PATTERN;
    }
    else if (sortPattern == sp_asc)
    {
        return ASC_SORT_PATTERN;
    }
    return "";
}

IE_NAMESPACE_END(config);

